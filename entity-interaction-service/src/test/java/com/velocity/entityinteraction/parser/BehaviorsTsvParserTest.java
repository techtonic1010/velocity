package com.velocity.entityinteraction.parser;

import com.velocity.entityinteraction.model.BehaviorRow;
import com.velocity.entityinteraction.model.BehaviorRow.ImpressionEntry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BehaviorsTsvParserTest {

    @Test
    void parsesAllFiveColumnsOfARealRow() throws IOException {
        String line = "1\tU131\t11/13/2019 8:36:57 AM\tN11 N22 N33\tN45-1 N46-0";

        try (Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(new StringReader(line), "TRAIN")) {
            BehaviorRow row = rows.toList().get(0);

            assertThat(row.impressionId()).isEqualTo("1");
            assertThat(row.userId()).isEqualTo("U131");
            assertThat(row.split()).isEqualTo("TRAIN");
            assertThat(row.time()).isEqualTo(LocalDateTime.of(2019, 11, 13, 8, 36, 57));
            assertThat(row.historyEntityIds()).containsExactly("N11", "N22", "N33");
            assertThat(row.impressions()).containsExactly(
                    new ImpressionEntry("N45", true),
                    new ImpressionEntry("N46", false));
        }
    }

    @Test
    void blankHistoryColumnParsesToEmptyList() throws IOException {
        String line = "2\tU200\t11/13/2019 9:05:58 AM\t\tN45-0";

        try (Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(new StringReader(line), "DEV")) {
            BehaviorRow row = rows.toList().get(0);
            assertThat(row.historyEntityIds()).isEmpty();
        }
    }

    @Test
    void blankLinesAreSkipped() throws IOException {
        String tsv = String.join("\n",
                "1\tU131\t11/13/2019 8:36:57 AM\tN11\tN45-1",
                "",
                "2\tU132\t11/13/2019 9:05:58 AM\tN12\tN46-0",
                "   ");

        try (Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(new StringReader(tsv), "TRAIN")) {
            assertThat(rows.toList()).hasSize(2);
        }
    }

    @Test
    void rowWithFewerThanFiveColumnsThrows() {
        String line = "1\tU131\t11/13/2019 8:36:57 AM\tN11";

        assertThatThrownBy(() -> {
            try (Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(new StringReader(line), "TRAIN")) {
                rows.toList();
            }
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void notZeroPaddedTimeIsParsedCorrectly() throws IOException {
        String line = "3\tU9\t1/5/2019 1:02:03 AM\tN1\tN2-1";

        try (Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(new StringReader(line), "TRAIN")) {
            BehaviorRow row = rows.toList().get(0);
            assertThat(row.time()).isEqualTo(LocalDateTime.of(2019, 1, 5, 1, 2, 3));
        }
    }

    @Test
    void multipleImpressionsPreserveOrderAndClickLabels() throws IOException {
        String line = "1\tU131\t11/13/2019 8:36:57 AM\tN11\tN1-0 N2-1 N3-0 N4-1";

        try (Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(new StringReader(line), "TRAIN")) {
            List<ImpressionEntry> impressions = rows.toList().get(0).impressions();
            assertThat(impressions).containsExactly(
                    new ImpressionEntry("N1", false),
                    new ImpressionEntry("N2", true),
                    new ImpressionEntry("N3", false),
                    new ImpressionEntry("N4", true));
        }
    }
}
