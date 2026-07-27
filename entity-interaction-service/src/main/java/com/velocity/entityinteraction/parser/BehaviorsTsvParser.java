package com.velocity.entityinteraction.parser;

import com.velocity.entityinteraction.model.BehaviorRow;
import com.velocity.entityinteraction.model.BehaviorRow.ImpressionEntry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * Parses MIND's behaviors.tsv: tab-separated, no header row.
 * Columns (verified against the real downloaded file): ImpressionID, UserID, Time, History,
 * Impressions. Time is e.g. "11/11/2019 9:05:58 AM" — not always zero-padded.
 */
public final class BehaviorsTsvParser {

    private static final int EXPECTED_COLUMNS = 5;
    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a", Locale.ENGLISH);

    private BehaviorsTsvParser() {
    }

    public static Stream<BehaviorRow> parse(Reader reader, String split) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(reader);
        return bufferedReader.lines()
                .filter(line -> !line.isBlank())
                .map(line -> parseLine(line, split));
    }

    private static BehaviorRow parseLine(String line, String split) {
        String[] columns = line.split("\t", -1);
        if (columns.length < EXPECTED_COLUMNS) {
            throw new IllegalArgumentException(
                    "behaviors.tsv row has fewer than " + EXPECTED_COLUMNS + " columns: " + line);
        }

        String impressionId = columns[0];
        String userId = columns[1];
        LocalDateTime time = LocalDateTime.parse(columns[2], TIME_FORMAT);
        List<String> historyEntityIds = parseHistory(columns[3]);
        List<ImpressionEntry> impressions = parseImpressions(columns[4]);

        return new BehaviorRow(impressionId, split, userId, time, historyEntityIds, impressions);
    }

    private static List<String> parseHistory(String rawHistory) {
        if (rawHistory == null || rawHistory.isBlank()) {
            return List.of();
        }
        return List.of(rawHistory.split(" "));
    }

    private static List<ImpressionEntry> parseImpressions(String rawImpressions) {
        List<ImpressionEntry> impressions = new ArrayList<>();
        for (String token : rawImpressions.split(" ")) {
            int dashIndex = token.lastIndexOf('-');
            String entityId = token.substring(0, dashIndex);
            boolean clicked = "1".equals(token.substring(dashIndex + 1));
            impressions.add(new ImpressionEntry(entityId, clicked));
        }
        return impressions;
    }
}
