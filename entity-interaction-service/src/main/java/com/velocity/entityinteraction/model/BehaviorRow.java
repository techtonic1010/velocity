package com.velocity.entityinteraction.model;

import java.time.LocalDateTime;
import java.util.List;

// One raw row of MIND's behaviors.tsv: ImpressionID, UserID, Time, History, Impressions.
// `split` ("TRAIN" or "DEV") is set by the parser, not read from the file — it's what lets sourceId
// stay unique across the two files, since ImpressionID alone collides between them.
public record BehaviorRow(
        String impressionId,
        String split,
        String userId,
        LocalDateTime time,
        List<String> historyEntityIds,
        List<ImpressionEntry> impressions) {

    public record ImpressionEntry(String entityId, boolean clicked) {
    }
}
