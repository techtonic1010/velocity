package com.velocity.entityinteraction.repository;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.dto.InteractionType;
import com.velocity.entityinteraction.util.ShardUtil;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class EntityHistoryRepositoryTest {

    @Test
    void upsertBatchSendsOneBatchUpdateWithCorrectSqlAndBatchSize() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryRepository repository = new EntityHistoryRepository(jdbcTemplate);
        InteractionEvent event = new InteractionEvent(
                "U131", "N45", InteractionType.CLICK, Instant.parse("2019-11-13T08:36:57Z"), "TRAIN-1");
        List<InteractionEvent> events = List.of(event);

        repository.upsertBatch(events, 8);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).batchUpdate(sqlCaptor.capture(), eq(events), eq(1), any());
        String sql = sqlCaptor.getValue();
        assertThat(sql).contains("INSERT INTO entity_history");
        assertThat(sql).contains("ON CONFLICT (source_id, entity_id) DO NOTHING");
    }

    @SuppressWarnings("unchecked")
    @Test
    void perRowSetterBindsAllSixColumnsInOrder() throws SQLException {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryRepository repository = new EntityHistoryRepository(jdbcTemplate);
        InteractionEvent event = new InteractionEvent(
                "U131", "N45", InteractionType.LIKE, Instant.parse("2019-11-13T08:36:57Z"), "TRAIN-1");
        List<InteractionEvent> events = List.of(event);

        repository.upsertBatch(events, 8);

        ArgumentCaptor<ParameterizedPreparedStatementSetter<InteractionEvent>> pssCaptor =
                ArgumentCaptor.forClass(ParameterizedPreparedStatementSetter.class);
        verify(jdbcTemplate).batchUpdate(anyString(), eq(events), eq(1), pssCaptor.capture());

        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        pssCaptor.getValue().setValues(preparedStatement, event);

        int expectedShard = ShardUtil.shardFor("U131", 8);
        verify(preparedStatement).setString(1, "TRAIN-1");
        verify(preparedStatement).setString(2, "N45");
        verify(preparedStatement).setString(3, "U131");
        verify(preparedStatement).setString(4, "LIKE");
        verify(preparedStatement).setTimestamp(5, Timestamp.from(Instant.parse("2019-11-13T08:36:57Z")));
        verify(preparedStatement).setInt(6, expectedShard);
    }
}
