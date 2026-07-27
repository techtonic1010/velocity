package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.dto.InteractionType;
import com.velocity.entityinteraction.model.BehaviorRow;
import com.velocity.entityinteraction.model.BehaviorRow.ImpressionEntry;
import com.velocity.entityinteraction.util.MurmurHash3;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class UserTimelineBuilderTest {

    private static final double DISLIKE_THRESHOLD = 0.05;
    private static final double LIKE_THRESHOLD = 0.15;

    // Mirrors UserTimelineBuilder's private classify() formula exactly, using the same public
    // MurmurHash3 utility, so tests can assert real-click labels without hardcoding magic values.
    private static InteractionType expectedType(String userId, String entityId) {
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

    @Test
    void historyEntriesProduceClickEventsInOriginalOrderBeforeRealClicks() {
        BehaviorRow row = new BehaviorRow(
                "1", "TRAIN", "U131",
                LocalDateTime.of(2019, 11, 13, 8, 36, 57),
                List.of("N11", "N22"),
                List.of(new ImpressionEntry("N45", true)));

        List<InteractionEvent> events = UserTimelineBuilder.build("U131", List.of(row));

        // 2 history events + 1 real click event.
        assertThat(events).hasSize(3);
        assertThat(events.get(0).entityId()).isEqualTo("N11");
        assertThat(events.get(0).sourceId()).isEqualTo("HIST-U131-0");
        assertThat(events.get(1).entityId()).isEqualTo("N22");
        assertThat(events.get(1).sourceId()).isEqualTo("HIST-U131-1");
        assertThat(events.get(0).interactionType()).isEqualTo(InteractionType.CLICK);
        assertThat(events.get(1).interactionType()).isEqualTo(InteractionType.CLICK);
        // History timestamps strictly precede each other and both precede the real click's row time.
        assertThat(events.get(0).timestamp()).isBefore(events.get(1).timestamp());
        assertThat(events.get(1).timestamp()).isBefore(events.get(2).timestamp());
    }

    @Test
    void onlyClickedImpressionsProduceRealClickEvents() {
        BehaviorRow row = new BehaviorRow(
                "1", "TRAIN", "U131",
                LocalDateTime.of(2019, 11, 13, 8, 36, 57),
                List.of(),
                List.of(new ImpressionEntry("N45", true), new ImpressionEntry("N46", false)));

        List<InteractionEvent> events = UserTimelineBuilder.build("U131", List.of(row));

        assertThat(events).hasSize(1);
        assertThat(events.get(0).entityId()).isEqualTo("N45");
        assertThat(events.get(0).sourceId()).isEqualTo("TRAIN-1");
    }

    @Test
    void realClickTypeMatchesTheDeterministicHashClassification() {
        BehaviorRow row = new BehaviorRow(
                "8821", "TRAIN", "U131",
                LocalDateTime.of(2019, 11, 13, 8, 36, 57),
                List.of(),
                List.of(new ImpressionEntry("N45", true)));

        List<InteractionEvent> events = UserTimelineBuilder.build("U131", List.of(row));

        assertThat(events.get(0).interactionType()).isEqualTo(expectedType("U131", "N45"));
    }

    @Test
    void sameUserEntityPairAlwaysClassifiesTheSameWayAcrossSeparateSessions() {
        BehaviorRow session1 = new BehaviorRow(
                "1", "TRAIN", "U131",
                LocalDateTime.of(2019, 11, 13, 8, 0, 0),
                List.of(),
                List.of(new ImpressionEntry("N45", true)));
        BehaviorRow session2 = new BehaviorRow(
                "2", "DEV", "U131",
                LocalDateTime.of(2019, 11, 14, 8, 0, 0),
                List.of(),
                List.of(new ImpressionEntry("N45", true)));

        List<InteractionEvent> events = UserTimelineBuilder.build("U131", List.of(session1, session2));

        assertThat(events).hasSize(2);
        assertThat(events.get(0).interactionType()).isEqualTo(events.get(1).interactionType());
    }

    @Test
    void historyEventsAreAlwaysClickRegardlessOfHashClassification() {
        // Pick an entityId whose (userId, entityId) pair would NOT classify as CLICK if it went
        // through classify() — History-sourced events must stay CLICK unconditionally anyway.
        String userId = "U999";
        String entityId = "N1";
        InteractionType wouldBeIfRealClick = expectedType(userId, entityId);

        BehaviorRow row = new BehaviorRow(
                "1", "TRAIN", userId,
                LocalDateTime.of(2019, 11, 13, 8, 36, 57),
                List.of(entityId),
                List.of());

        List<InteractionEvent> events = UserTimelineBuilder.build(userId, List.of(row));

        assertThat(events).hasSize(1);
        assertThat(events.get(0).interactionType()).isEqualTo(InteractionType.CLICK);
        // Documents the intentional divergence when the hash-classification would say otherwise.
        if (wouldBeIfRealClick != InteractionType.CLICK) {
            assertThat(events.get(0).interactionType()).isNotEqualTo(wouldBeIfRealClick);
        }
    }

    @Test
    void rowsAreSortedByTimeBeforeHistoryAndRealClickEventsAreBuilt() {
        BehaviorRow later = new BehaviorRow(
                "2", "TRAIN", "U1",
                LocalDateTime.of(2019, 11, 14, 8, 0, 0),
                List.of("N1"),
                List.of(new ImpressionEntry("N9", true)));
        BehaviorRow earlier = new BehaviorRow(
                "1", "TRAIN", "U1",
                LocalDateTime.of(2019, 11, 13, 8, 0, 0),
                List.of("N2"),
                List.of(new ImpressionEntry("N8", true)));

        // Passed in out-of-order deliberately.
        List<InteractionEvent> events = UserTimelineBuilder.build("U1", List.of(later, earlier));

        // History is sourced from the earliest row after sorting, i.e. "earlier" (History="N2").
        InteractionEvent historyEvent = events.stream()
                .filter(e -> e.sourceId().startsWith("HIST-"))
                .findFirst().orElseThrow();
        assertThat(historyEvent.entityId()).isEqualTo("N2");

        // Real click events themselves must appear in ascending row-time order.
        List<InteractionEvent> realClicks = events.stream()
                .filter(e -> !e.sourceId().startsWith("HIST-"))
                .sorted(Comparator.comparing(InteractionEvent::timestamp))
                .toList();
        List<InteractionEvent> realClicksAsBuilt = events.stream()
                .filter(e -> !e.sourceId().startsWith("HIST-"))
                .toList();
        assertThat(realClicksAsBuilt).isEqualTo(realClicks);
        assertThat(realClicksAsBuilt.get(0).sourceId()).isEqualTo("TRAIN-1");
        assertThat(realClicksAsBuilt.get(1).sourceId()).isEqualTo("TRAIN-2");
    }

    @Test
    void emptyHistoryProducesNoHistoryEvents() {
        BehaviorRow row = new BehaviorRow(
                "1", "TRAIN", "U1",
                LocalDateTime.of(2019, 11, 13, 8, 0, 0),
                List.of(),
                List.of(new ImpressionEntry("N1", true)));

        List<InteractionEvent> events = UserTimelineBuilder.build("U1", List.of(row));

        assertThat(events).noneMatch(e -> e.sourceId().startsWith("HIST-"));
    }
}
