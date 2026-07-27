package com.velocity.entityinteraction.controller;

import com.velocity.entityinteraction.service.ReplayService;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ReplayControllerTest {

    @Test
    void withUserIdPresentDelegatesToReplayUserAndIgnoresLimit() {
        ReplayService replayService = mock(ReplayService.class);
        ReplayService.ReplayResult expected = new ReplayService.ReplayResult(1, 4);
        when(replayService.replayUser("U131")).thenReturn(expected);
        ReplayController controller = new ReplayController(replayService);

        ReplayService.ReplayResult result = controller.replay("U131", 100);

        assertThat(result).isEqualTo(expected);
        verify(replayService).replayUser("U131");
        verify(replayService, never()).replayAll(anyInt());
    }

    @Test
    void withoutUserIdDelegatesToReplayAllWithTheGivenLimit() {
        ReplayService replayService = mock(ReplayService.class);
        ReplayService.ReplayResult expected = new ReplayService.ReplayResult(5, 20);
        when(replayService.replayAll(50)).thenReturn(expected);
        ReplayController controller = new ReplayController(replayService);

        ReplayService.ReplayResult result = controller.replay(null, 50);

        assertThat(result).isEqualTo(expected);
        verify(replayService).replayAll(50);
        verify(replayService, never()).replayUser(anyString());
    }

    @Test
    void withNeitherUserIdNorLimitDelegatesToReplayAllWithNullLimit() {
        ReplayService replayService = mock(ReplayService.class);
        ReplayService.ReplayResult expected = new ReplayService.ReplayResult(10, 40);
        when(replayService.replayAll(null)).thenReturn(expected);
        ReplayController controller = new ReplayController(replayService);

        ReplayService.ReplayResult result = controller.replay(null, null);

        assertThat(result).isEqualTo(expected);
        verify(replayService).replayAll(null);
    }
}
