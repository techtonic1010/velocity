package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.dto.InteractionType;
import com.velocity.entityinteraction.model.BehaviorRow;
import com.velocity.entityinteraction.model.BehaviorRow.ImpressionEntry;
import com.velocity.entityinteraction.util.MurmurHash3;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Pure logic, no I/O: turns one user's raw behaviors.tsv rows (both splits combined) into the
 * merged, chronological list of InteractionEvents that gets published to Kafka.
 */
public final class UserTimelineBuilder {

    private static final double DISLIKE_THRESHOLD = 0.05;
    private static final double LIKE_THRESHOLD = 0.15;

    private UserTimelineBuilder() {
    }

    public static List<InteractionEvent> build(String userId, List<BehaviorRow> rows) {
        List<BehaviorRow> sorted = rows.stream()
                .sorted(Comparator.comparing(BehaviorRow::time))
                .toList();

        List<InteractionEvent> events = new ArrayList<>();
        events.addAll(buildHistoryEvents(userId, sorted));
        events.addAll(buildRealClickEvents(userId, sorted));
        return events;
    }

    private static List<InteractionEvent> buildHistoryEvents(String userId, List<BehaviorRow> sortedRows) {
        // History is confirmed frozen across all of a user's rows (spot-checked against real data) —
        // the earliest row after sorting is as good a source as any, they're all identical.
        List<String> historyEntityIds = sortedRows.get(0).historyEntityIds();
        // Anchors to the earliest impression row's Time (not earliest click) — always defined, even
        // for a user whose impressions never got a real click.
        Instant anchor = sortedRows.get(0).time().toInstant(ZoneOffset.UTC);

        int size = historyEntityIds.size();
        List<InteractionEvent> events = new ArrayList<>(size);
        for (int index = 0; index < size; index++) {
            String entityId = historyEntityIds.get(index);
            Instant timestamp = anchor.minusSeconds(size - index);
            String sourceId = "HIST-" + userId + "-" + index;
            events.add(new InteractionEvent(userId, entityId, InteractionType.CLICK, timestamp, sourceId));
        }
        return events;
    }

    private static List<InteractionEvent> buildRealClickEvents(String userId, List<BehaviorRow> sortedRows) {
        List<InteractionEvent> events = new ArrayList<>();
        for (BehaviorRow row : sortedRows) {
            Instant timestamp = row.time().toInstant(ZoneOffset.UTC);
            for (ImpressionEntry impression : row.impressions()) {
                if (!impression.clicked()) {
                    continue;
                }
                String sourceId = row.split() + "-" + row.impressionId();
                InteractionType type = classify(userId, impression.entityId());
                events.add(new InteractionEvent(userId, impression.entityId(), type, timestamp, sourceId));
            }
        }
        return events;
    }

    /**
     * Deterministic LIKE/DISLIKE simulation, real clicks only — History-derived events are always
     * plain CLICK (handled above). Reuses the same MurmurHash3 primitive the Bloom filter uses.
     * Same (userId, entityId) pair always yields the same label, since a real pair can be clicked in
     * two separate real sessions and both must agree.
     */
    private static InteractionType classify(String userId, String entityId) {
        String key = userId + "|" + entityId;
        int hash = MurmurHash3.hash32(key.getBytes(StandardCharsets.UTF_8), 0);
        double bucket = (hash & 0xFFFFFFFFL) / 4294967296.0;
        if (bucket < DISLIKE_THRESHOLD) {
            return InteractionType.DISLIKE;
        }
        if (bucket < LIKE_THRESHOLD) {
            return InteractionType.LIKE;
        }
        return InteractionType.CLICK;
    }
}
