package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

// ReplayService takes its TSV paths as plain constructor strings (raw @Value injection), so it can
// be unit-tested directly against real temp files without a Spring context or Kafka/Postgres/Redis.
class ReplayServiceTest {

    @TempDir
    Path tempDir;

    private Path trainTsv;
    private Path devTsv;

    @BeforeEach
    void writeFixtureFiles() throws IOException {
        // U1: History N11,N22 in the earliest (TRAIN) row, real click N45 in TRAIN, real click N50 in DEV.
        // U2: History N30, real click N31 in TRAIN only.
        trainTsv = tempDir.resolve("train-behaviors.tsv");
        Files.writeString(trainTsv, String.join("\n",
                "1\tU1\t11/13/2019 8:36:57 AM\tN11 N22\tN45-1 N46-0",
                "2\tU2\t11/13/2019 9:00:00 AM\tN30\tN31-1"));

        devTsv = tempDir.resolve("dev-behaviors.tsv");
        Files.writeString(devTsv, "3\tU1\t11/14/2019 8:00:00 AM\t\tN50-1");
    }

    @Test
    void replayUserMergesRowsFromBothSplitsInChronologicalOrder() {
        InteractionEventProducer producer = mock(InteractionEventProducer.class);
        ReplayService replayService = new ReplayService(producer, trainTsv.toString(), devTsv.toString());

        ReplayService.ReplayResult result = replayService.replayUser("U1");

        // 2 History events (N11, N22) + 1 TRAIN real click (N45) + 1 DEV real click (N50).
        assertThat(result.usersReplayed()).isEqualTo(1);
        assertThat(result.eventsPublished()).isEqualTo(4);

        ArgumentCaptor<InteractionEvent> captor = ArgumentCaptor.forClass(InteractionEvent.class);
        verify(producer, times(4)).publish(captor.capture());
        List<InteractionEvent> published = captor.getAllValues();

        assertThat(published).extracting(InteractionEvent::sourceId)
                .containsExactly("HIST-U1-0", "HIST-U1-1", "TRAIN-1", "DEV-3");
        assertThat(published).extracting(InteractionEvent::entityId)
                .containsExactly("N11", "N22", "N45", "N50");
        assertThat(published).allMatch(event -> event.userId().equals("U1"));
    }

    @Test
    void replayUserForAnUnknownUserPublishesNothing() {
        InteractionEventProducer producer = mock(InteractionEventProducer.class);
        ReplayService replayService = new ReplayService(producer, trainTsv.toString(), devTsv.toString());

        ReplayService.ReplayResult result = replayService.replayUser("U-does-not-exist");

        assertThat(result.usersReplayed()).isZero();
        assertThat(result.eventsPublished()).isZero();
        verifyNoInteractions(producer);
    }

    @Test
    void replayAllWithoutLimitCoversEveryUserInBothFiles() {
        InteractionEventProducer producer = mock(InteractionEventProducer.class);
        ReplayService replayService = new ReplayService(producer, trainTsv.toString(), devTsv.toString());

        ReplayService.ReplayResult result = replayService.replayAll(null);

        // U1: 4 events (as above). U2: 1 History event (N30) + 1 TRAIN real click (N31) = 2 events.
        assertThat(result.usersReplayed()).isEqualTo(2);
        assertThat(result.eventsPublished()).isEqualTo(6);
        verify(producer, times(6)).publish(any());
    }

    @Test
    void replayAllWithLimitOneOnlyReadsTheFirstRowOfEachFile() {
        InteractionEventProducer producer = mock(InteractionEventProducer.class);
        ReplayService replayService = new ReplayService(producer, trainTsv.toString(), devTsv.toString());

        // limit=1 -> only row 1 (U1) from train, only row 3 (U1) from dev; U2's train row 2 is skipped.
        ReplayService.ReplayResult result = replayService.replayAll(1);

        assertThat(result.usersReplayed()).isEqualTo(1);
        assertThat(result.eventsPublished()).isEqualTo(4);
    }
}
