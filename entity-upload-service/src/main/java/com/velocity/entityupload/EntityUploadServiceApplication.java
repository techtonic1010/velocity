package com.velocity.entityupload;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EntityUploadServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(EntityUploadServiceApplication.class, args);
    }
}

// Good, hold off on the large one then — I'll wait for MIND-small. Let's dig into the concurrency design properly.

// What "6 concurrent calls" physically means: Executors.newFixedThreadPool(embedConcurrency) in IngestionService.ingest() creates a pool of exactly 6 real OS threads that live for the duration of one /ingest call. Each of those threads is capable of doing one blocking thing at a time — in our case, one blocking HTTP call to embedding-creator via EmbeddingCreatorClient.embed(). So "6 concurrent" literally means: at most 6 HTTP requests to embedding-creator are ever in flight from this app at the same instant.

// How the other 94 articles in a 100-row batch don't just vanish: in processBatch, the loop executor.submit(() -> embedOne(article)) runs 100 times immediately — submission itself is non-blocking, so all 100 tasks get handed to the pool right away, each returning a Future instantly. But the pool only has 6 threads, so only 6 of those 100 tasks are actually running at once; the other 94 sit in the pool's internal queue. The moment one of the 6 threads finishes its HTTP call, it automatically pulls the next queued task and starts it — this queueing/dispatch is built into ThreadPoolExecutor (what newFixedThreadPool gives you), we didn't write any of that scheduling logic ourselves.

// Why future.get() being called in submission order doesn't waste the concurrency: the second loop in processBatch calls future.get() on futures 1, 2, 3... in the order they were submitted — but that's just the order we check results in, not the order they actually finish in. If article #47's embed call happens to finish before #1's (plausible — network timing varies), it just sits in its already-completed Future waiting for us to get around to asking. All 6 (then rotating through 100) requests are genuinely running concurrently regardless of this checking order.

// Why 6 specifically, and why not more: embedding-creator is one Python process running one loaded model. Model inference (_model.encode(...)) is CPU-bound — it's not like a database that can genuinely serve many parallel queries faster with more connections. If we set embedConcurrency to, say, 50, we wouldn't get 50x throughput — we'd just have 50 requests queued up on FastAPI/Uvicorn's side waiting for the same CPU to free up, with no benefit and more memory/connection overhead on both sides. 6 is a deliberately modest number chosen to keep a few requests overlapping (so one thread isn't sitting completely idle waiting on pure network round-trip time while nothing else happens), without pretending the single-process embedder can actually parallelize real work. Worth being honest: 6 wasn't benchmarked — it's a reasonable starting default for "a handful of overlapping calls to one CPU-bound process," not a number derived from measuring actual throughput.

// One knob you already have for free: embed-concurrency: 6 lives in application.yml, not hardcoded in IngestionService — so if you ever want to experiment (e.g., try 3, or 12) once we're running a real ingest against actual data, it's a one-line config change, no code edit needed.
///////////////////////////////////////////////////////
/// summary : 

/// A thread pool is like a team of 6 workers: you give them all 100 jobs
//  immediately, only 6 work at any moment, the rest wait in a queue, and 
// whenever a worker finishes, it automatically picks up the next waiting
//  job until everything is complete.

///////////////////////////////////////////////////////