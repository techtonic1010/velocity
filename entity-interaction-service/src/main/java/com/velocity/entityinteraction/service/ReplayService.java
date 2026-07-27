package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.model.BehaviorRow;
import com.velocity.entityinteraction.parser.BehaviorsTsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

// Reads MIND's real behaviors.tsv (both splits) and replays it through Kafka as if arriving live —
// the "interaction replay" scope decision from PROJECT_SPEC.md §2. Only talks to
// InteractionEventProducer directly (in-process), never loops back over HTTP to POST /interactions.
@Service
public class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);

    private final InteractionEventProducer producer;
    private final Path trainTsvPath;
    private final Path devTsvPath;

    public ReplayService(
            InteractionEventProducer producer,
            @Value("${mind.behaviors-train-tsv-path}") String trainTsvPath,
            @Value("${mind.behaviors-dev-tsv-path}") String devTsvPath) {
        this.producer = producer;
        this.trainTsvPath = Path.of(trainTsvPath);
        this.devTsvPath = Path.of(devTsvPath);
    }

    /**
     * Replays exactly one real user's click history — Milestone 4's literal verify criterion.
     * Scans both splits for rows belonging to userId, builds the merged timeline, publishes
     * every event in order.
     */
    public ReplayResult replayUser(String userId) {
        List<BehaviorRow> rows = new ArrayList<>();
        rows.addAll(readRowsForUser(trainTsvPath, "TRAIN", userId));
        rows.addAll(readRowsForUser(devTsvPath, "DEV", userId));

        if (rows.isEmpty()) {
            return new ReplayResult(0, 0);
        }

        List<InteractionEvent> events = UserTimelineBuilder.build(userId, rows);
        events.forEach(producer::publish);
        log.info("Replayed userId={}: {} events published", userId, events.size());
        return new ReplayResult(1, events.size());
    }

    /**
     * Bulk replay, bounded by limit (mirrors entity-upload-service's ?limit= convention) — reads
     * up to limit rows from each split, groups by user, replays every user found.
     */
    public ReplayResult replayAll(Integer limit) {
        Map<String, List<BehaviorRow>> byUser = new LinkedHashMap<>();
        readRows(trainTsvPath, "TRAIN", limit, byUser);
        readRows(devTsvPath, "DEV", limit, byUser);

        int eventsPublished = 0;
        for (Map.Entry<String, List<BehaviorRow>> entry : byUser.entrySet()) {
            List<InteractionEvent> events = UserTimelineBuilder.build(entry.getKey(), entry.getValue());
            events.forEach(producer::publish);
            eventsPublished += events.size();
        }
        log.info("Replayed {} users, {} events published", byUser.size(), eventsPublished);
        return new ReplayResult(byUser.size(), eventsPublished);
    }

    private List<BehaviorRow> readRowsForUser(Path path, String split, String userId) {
        try (FileReader reader = new FileReader(path.toFile());
             Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(reader, split)) {
            return rows.filter(row -> row.userId().equals(userId)).toList();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read behaviors.tsv at " + path, e);
        }
    }

    private void readRows(Path path, String split, Integer limit, Map<String, List<BehaviorRow>> byUser) {
        try (FileReader reader = new FileReader(path.toFile());
             Stream<BehaviorRow> stream = BehaviorsTsvParser.parse(reader, split)) {
            Stream<BehaviorRow> bounded = limit != null ? stream.limit(limit) : stream;
            bounded.forEach(row -> byUser.computeIfAbsent(row.userId(), id -> new ArrayList<>()).add(row));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read behaviors.tsv at " + path, e);
        }
    }

    public record ReplayResult(int usersReplayed, int eventsPublished) {
    }
}
