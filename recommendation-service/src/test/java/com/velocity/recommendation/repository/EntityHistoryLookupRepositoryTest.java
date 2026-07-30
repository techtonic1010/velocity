package com.velocity.recommendation.repository;

import com.velocity.recommendation.dto.InteractionType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class EntityHistoryLookupRepositoryTest {

    @Test
    void findSeenEntityIdsWithEmptyCandidatesNeverQueries() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryLookupRepository repository = new EntityHistoryLookupRepository(jdbcTemplate);

        assertThat(repository.findSeenEntityIds("U1", List.of())).isEmpty();
        verifyNoInteractions(jdbcTemplate);
    }

    @SuppressWarnings("unchecked")
    @Test
    void findSeenEntityIdsBindsUserIdAndArrayAndCollectsResults() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryLookupRepository repository = new EntityHistoryLookupRepository(jdbcTemplate);

        when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(RowMapper.class)))
                .thenAnswer(invocation -> {
                    RowMapper<String> rowMapper = invocation.getArgument(2);
                    ResultSet rs = mock(ResultSet.class);
                    when(rs.getString("entity_id")).thenReturn("N1");
                    return List.of(rowMapper.mapRow(rs, 0));
                });

        Set<String> result = repository.findSeenEntityIds("U1", List.of("N1", "N2"));

        assertThat(result).containsExactly("N1");

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<PreparedStatementSetter> pssCaptor = ArgumentCaptor.forClass(PreparedStatementSetter.class);
        verify(jdbcTemplate).query(sqlCaptor.capture(), pssCaptor.capture(), any(RowMapper.class));
        assertThat(sqlCaptor.getValue()).contains("SELECT DISTINCT entity_id FROM entity_history")
                .contains("WHERE user_id = ? AND entity_id = ANY(?)");

        PreparedStatement ps = mock(PreparedStatement.class);
        Connection connection = mock(Connection.class);
        Array array = mock(Array.class);
        when(ps.getConnection()).thenReturn(connection);
        when(connection.createArrayOf(eq("varchar"), any())).thenReturn(array);
        pssCaptor.getValue().setValues(ps);
        verify(ps).setString(1, "U1");
        verify(ps).setArray(2, array);
    }

    @Test
    void findRecentEntityIdsWithEmptyResultReturnsEmptyList() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryLookupRepository repository = new EntityHistoryLookupRepository(jdbcTemplate);
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("U1"), eq(5))).thenReturn(List.of());

        List<String> result = repository.findRecentEntityIds("U1", 5);

        assertThat(result).isEmpty();
    }

    @Test
    void findRecentEntityIdsUsesDistinctOnPerEntityBeforeLimiting() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryLookupRepository repository = new EntityHistoryLookupRepository(jdbcTemplate);
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("U1"), eq(5)))
                .thenReturn(List.of("N9", "N3"));

        List<String> result = repository.findRecentEntityIds("U1", 5);

        assertThat(result).containsExactly("N9", "N3");
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).query(sqlCaptor.capture(), any(RowMapper.class), eq("U1"), eq(5));
        assertThat(sqlCaptor.getValue()).contains("DISTINCT ON (entity_id)").contains("ORDER BY event_timestamp DESC");
    }

    @Test
    void findLatestInteractionTypesWithEmptySeedsNeverQueries() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryLookupRepository repository = new EntityHistoryLookupRepository(jdbcTemplate);

        assertThat(repository.findLatestInteractionTypes("U1", List.of())).isEmpty();
        verifyNoInteractions(jdbcTemplate);
    }

    @SuppressWarnings("unchecked")
    @Test
    void findLatestInteractionTypesMapsEntityIdToInteractionType() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryLookupRepository repository = new EntityHistoryLookupRepository(jdbcTemplate);

        doAnswer(invocation -> {
            RowCallbackHandler handler = invocation.getArgument(2);
            ResultSet rs = mock(ResultSet.class);
            when(rs.getString("entity_id")).thenReturn("N1");
            when(rs.getString("interaction_type")).thenReturn("LIKE");
            handler.processRow(rs);
            return null;
        }).when(jdbcTemplate).query(anyString(), any(PreparedStatementSetter.class), any(RowCallbackHandler.class));

        Map<String, InteractionType> result = repository.findLatestInteractionTypes("U1", List.of("N1"));

        assertThat(result).containsExactly(Map.entry("N1", InteractionType.LIKE));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).query(sqlCaptor.capture(), any(PreparedStatementSetter.class), any(RowCallbackHandler.class));
        assertThat(sqlCaptor.getValue()).contains("DISTINCT ON (entity_id) entity_id, interaction_type");
    }
}
