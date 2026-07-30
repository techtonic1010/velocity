package com.velocity.recommendation.repository;

import com.velocity.recommendation.repository.EntityLookupRepository.EntitySummary;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class EntityLookupRepositoryTest {

    @Test
    void emptyIdListNeverQueries() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityLookupRepository repository = new EntityLookupRepository(jdbcTemplate);

        List<EntitySummary> result = repository.findByIds(List.of());

        assertThat(result).isEmpty();
        verifyNoInteractions(jdbcTemplate);
    }

    @SuppressWarnings("unchecked")
    @Test
    void findByIdsBindsAnArrayParameterAndMapsEveryColumn() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityLookupRepository repository = new EntityLookupRepository(jdbcTemplate);
        List<String> ids = List.of("N1", "N2");

        repository.findByIds(ids);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<PreparedStatementSetter> pssCaptor = ArgumentCaptor.forClass(PreparedStatementSetter.class);
        ArgumentCaptor<RowMapper<EntitySummary>> rowMapperCaptor = ArgumentCaptor.forClass(RowMapper.class);
        verify(jdbcTemplate).query(sqlCaptor.capture(), pssCaptor.capture(), rowMapperCaptor.capture());

        assertThat(sqlCaptor.getValue()).contains("SELECT entity_id, title, category FROM entities")
                .contains("WHERE entity_id = ANY(?)");

        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        Connection connection = mock(Connection.class);
        Array array = mock(Array.class);
        when(preparedStatement.getConnection()).thenReturn(connection);
        when(connection.createArrayOf(eq("varchar"), any())).thenReturn(array);

        pssCaptor.getValue().setValues(preparedStatement);

        ArgumentCaptor<Object[]> arrayContentsCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(connection).createArrayOf(eq("varchar"), arrayContentsCaptor.capture());
        assertThat(arrayContentsCaptor.getValue()).containsExactly("N1", "N2");
        verify(preparedStatement).setArray(1, array);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("entity_id")).thenReturn("N1");
        when(resultSet.getString("title")).thenReturn("Some Title");
        when(resultSet.getString("category")).thenReturn("sports");

        EntitySummary mapped = rowMapperCaptor.getValue().mapRow(resultSet, 0);
        assertThat(mapped).isEqualTo(new EntitySummary("N1", "Some Title", "sports"));
    }
}
