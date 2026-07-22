package com.velocity.entityupload.service;

import com.velocity.entityupload.client.EmbeddingCreatorClient;
import com.velocity.entityupload.dto.EmbedResponse;
import com.velocity.entityupload.model.NewsArticle;
import com.velocity.entityupload.parser.MindNewsTsvParser;
import com.velocity.entityupload.repository.EntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

// The orchestrator for Deliverable B: reads news.tsv, gets each article embedded by
// embedding-creator, and writes the results into Postgres. This is the only class that
// talks to both EmbeddingCreatorClient and EntityRepository — neither of those two talk
// to each other directly.
@Service
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final EmbeddingCreatorClient embeddingCreatorClient;
    private final EntityRepository entityRepository;
    private final Path newsTsvPath;
    private final int batchSize;
    private final int embedConcurrency;

    public IngestionService(
            EmbeddingCreatorClient embeddingCreatorClient,
            EntityRepository entityRepository,
            @Value("${mind.news-tsv-path}") String newsTsvPath,
            @Value("${ingestion.batch-size}") int batchSize,
            @Value("${ingestion.embed-concurrency}") int embedConcurrency) {
        this.embeddingCreatorClient = embeddingCreatorClient;
        this.entityRepository = entityRepository;
        this.newsTsvPath = Path.of(newsTsvPath);
        this.batchSize = batchSize;
        this.embedConcurrency = embedConcurrency;
    }

    /**
     * Entry point, called by IngestController. Reads news.tsv (optionally capped at
     * {@code limit} rows), processes it in batches of {@code batchSize}, and returns a
     * summary of how many succeeded/failed.
     */
    public IngestionResult ingest(Integer limit) {
        // The thread pool that bounds how many /embed calls are in flight at once.
        // embedConcurrency (~6) is deliberately small: embedding-creator is one CPU-bound
        // Python process, so throwing more concurrent requests at it than that doesn't make
        // it faster, it just makes requests queue up on its side. This pool is created once
        // per /ingest call (not shared across calls) and is shut down in the `finally` block
        // below, whether ingestion succeeds or blows up.
        ExecutorService executor = Executors.newFixedThreadPool(embedConcurrency);

        // try-with-resources: both the file and the Stream get closed automatically,
        // even if an exception is thrown partway through.
        try (FileReader reader = new FileReader( newsTsvPath.toFile() );
             Stream<NewsArticle> articles = MindNewsTsvParser.parse(reader)) {

            // The parser gives us a lazy stream — nothing has been read from disk yet.
            // Optionally cap it at `limit` rows (used for fast verification runs, e.g.
            // POST /ingest?limit=20, instead of waiting on the full ~50k-row file).
            Stream<NewsArticle> bounded = limit != null ? articles.limit(limit) : articles;
            Iterator<NewsArticle> iterator = bounded.iterator();

            int total = 0;
            int succeeded = 0;
            List<String> failedIds = new ArrayList<>();
            List<NewsArticle> batch = new ArrayList<>(batchSize);

            // Pull articles out of the stream one at a time, accumulating them into
            // an in-memory batch. Once the batch hits batchSize (100), process it as a
            // unit (embed + write) and start a new empty batch.
            while (iterator.hasNext()) {
                batch.add(iterator.next());
                total++;
                if (batch.size() == batchSize) {
                    BatchOutcome outcome = processBatch(batch, executor);
                    succeeded += outcome.succeeded();
                    failedIds.addAll(outcome.failedIds());
                    log.info("Ingested {} articles so far", total);
                    batch.clear();
                }
            }
            // The file's row count won't always be a multiple of batchSize — process
            // whatever's left over in the final, possibly-smaller batch.
            if (!batch.isEmpty()) {
                BatchOutcome outcome = processBatch(batch, executor);
                succeeded += outcome.succeeded();
                failedIds.addAll(outcome.failedIds());
            }

            return new IngestionResult(total, succeeded, failedIds.size(), failedIds);
        } catch (IOException e) {
            // Can't read the file at all (bad path, permissions) — nothing to recover from,
            // so this is a hard failure of the whole /ingest call, not a per-row skip.
            throw new IllegalStateException("Failed to read MIND news.tsv at " + newsTsvPath, e);
        } finally {
            // Always release the thread pool's threads, whether ingest() returned
            // normally or an exception propagated out of the try block above.
            executor.shutdown();
        }
    }

    /**
     * Embeds every article in one batch (bounded concurrency via the shared executor),
     * then writes everything that succeeded to Postgres in a single upsert call.
     * One bad article does not fail the whole batch.
     */
    private BatchOutcome processBatch(List<NewsArticle> batch, ExecutorService executor) {
        // Submit all embed calls for this batch to the executor up front. Because the
        // pool only has `embedConcurrency` threads, at most that many of these actually
        // run at the same time — the rest queue until a thread frees up. Each submit()
        // returns immediately with a Future; nothing has necessarily finished yet.
        List<Future<EmbeddedArticle>> futures = new ArrayList<>(batch.size());
        for (NewsArticle article : batch) {
            futures.add(executor.submit(() -> embedOne(article)));
        }

        List<EntityRepository.EntityRow> rows = new ArrayList<>(batch.size());
        List<String> failedIds = new ArrayList<>();
        int succeeded = 0;

        // future.get() blocks until that specific article's embed call is done (or
        // already is done, if it finished while we were still submitting others).
        // Looping over the futures in submission order just means we wait for them in
        // that order — it does not force them to have run in that order.
        for (Future<EmbeddedArticle> future : futures) {
            try {
                EmbeddedArticle result = future.get();
                rows.add(new EntityRepository.EntityRow(result.article(), result.vectorBytes()));
                succeeded++;
            } catch (ExecutionException e) {
                // The Callable (embedOne) threw — future.get() wraps whatever it threw
                // inside ExecutionException, so the real error is in getCause().
                EmbedFailure failure = (EmbedFailure) e.getCause();
                log.warn("Embedding failed for entityId={}: {}",
                        failure.entityId(), failure.getCause().getMessage());
                // Deliberately NOT rethrown: one failed article is logged and skipped,
                // the rest of the batch still gets written.
                failedIds.add(failure.entityId());
            } catch (InterruptedException e) {
                // Someone interrupted this thread (e.g. app shutdown) while we were
                // waiting on future.get(). Restore the interrupt flag for callers further
                // up the stack, then abort — this is not a per-row failure, it's the
                // whole operation being cancelled.
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while ingesting batch", e);
            }
        }

        // One JDBC batch upsert for everything that succeeded in this batch, not one
        // write per row — this is the actual mechanism behind "batch size 100".
        if (!rows.isEmpty()) {
            entityRepository.upsertBatch(rows);
        }
        return new BatchOutcome(succeeded, failedIds);
    }

    /**
     * Embeds a single article by calling embedding-creator. Runs on one of the
     * executor's threads, so this method body is what actually executes concurrently.
     */
    private EmbeddedArticle embedOne(NewsArticle article) {
        try {
            // EmbeddingCreatorClient already retries once internally on transient
            // failure (see EmbeddingCreatorClient.MAX_ATTEMPTS) — if we get an exception
            // here, that retry already happened and failed too.
            EmbedResponse response =
                    embeddingCreatorClient.embed(article.newsId(), article.embeddingText());
            byte[] vectorBytes = EntityRepository.packVector(response.vector());
            return new EmbeddedArticle(article, vectorBytes);
        } catch (RuntimeException e) {
            // Wrap the real failure with which entityId caused it, so processBatch()
            // can log and skip the right article instead of just "something failed".
            throw new EmbedFailure(article.newsId(), e);
        }
    }

    // One article paired with its packed vector bytes, ready to become an EntityRow.
    private record EmbeddedArticle(NewsArticle article, byte[] vectorBytes) {
    }

    // Result of processing exactly one batch — how many of its articles made it into
    // Postgres, and the entityIds of the ones that didn't.
    private record BatchOutcome(int succeeded, List<String> failedIds) {
    }

    // Carries which entityId failed alongside the real underlying exception, so it can
    // travel through Future/ExecutionException and still be logged meaningfully.
    private static final class EmbedFailure extends RuntimeException {
        private final String entityId;

        private EmbedFailure(String entityId, Throwable cause) {
            super(cause);
            this.entityId = entityId;
        }

        private String entityId() {
            return entityId;
        }
    }

    // The response body of POST /ingest — a summary of the whole run, not per-batch.
    public record IngestionResult(int total, int succeeded, int failed, List<String> failedIds) {
    }
}

// TSV File
//    │
//    ▼
// Read articles
//    │
//    ▼
// Make batches (100)
//    │
//    ▼
// Ask Python to create embeddings
//    │
//    ▼
// Convert embeddings to bytes
//    │
//    ▼
// Store batch in PostgreSQL
//    │
//    ▼
// Repeat
//    │
//    ▼
// Return summary

// 3. Which one affects recommendation latency?
// ------------------------------------------------------------

// Neither.

// Recommendation latency depends on this:

// Recommendation Service
//         │
//         ▼
// Redis
// Neighbor Index

// It does **not** depend on whether the `/ingest` endpoint waited 8 minutes or returned immediately.

// Once ingestion has completed, both designs produce exactly the same stored data.
///////////////////////////////////
/// 
/// 
/// 
// both the services are entirely decoupled from each other, and the ingestion
//  service is a one-time batch job that runs in the background, while the 
// recommendation service is a real-time service that serves requests from users.