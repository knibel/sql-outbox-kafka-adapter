package de.knibel.outbox.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ResultSetConverter}.
 */
class ResultSetConverterTest {

    @Test
    void toMap_convertsSingleColumn() throws Exception {
        ResultSetMetaData meta = mock(ResultSetMetaData.class);
        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnLabel(1)).thenReturn("order_id");

        ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(meta);
        when(rs.getObject(1)).thenReturn("ORD-001");

        Map<String, Object> row = ResultSetConverter.toMap(rs);

        assertThat(row).hasSize(1)
                       .containsEntry("order_id", "ORD-001");
    }

    @Test
    void toMap_convertsMultipleColumns() throws Exception {
        ResultSetMetaData meta = mock(ResultSetMetaData.class);
        when(meta.getColumnCount()).thenReturn(3);
        when(meta.getColumnLabel(1)).thenReturn("id");
        when(meta.getColumnLabel(2)).thenReturn("name");
        when(meta.getColumnLabel(3)).thenReturn("amount");

        ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(meta);
        when(rs.getObject(1)).thenReturn(42);
        when(rs.getObject(2)).thenReturn("John");
        when(rs.getObject(3)).thenReturn(99.95);

        Map<String, Object> row = ResultSetConverter.toMap(rs);

        assertThat(row).hasSize(3)
                       .containsEntry("id", 42)
                       .containsEntry("name", "John")
                       .containsEntry("amount", 99.95);
    }

    @Test
    void toMap_preservesColumnLabelCasing() throws Exception {
        ResultSetMetaData meta = mock(ResultSetMetaData.class);
        when(meta.getColumnCount()).thenReturn(2);
        when(meta.getColumnLabel(1)).thenReturn("ORDER_ID");
        when(meta.getColumnLabel(2)).thenReturn("customerName");

        ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(meta);
        when(rs.getObject(1)).thenReturn("ORD-001");
        when(rs.getObject(2)).thenReturn("John");

        Map<String, Object> row = ResultSetConverter.toMap(rs);

        assertThat(row).containsKey("ORDER_ID")
                       .containsKey("customerName")
                       .doesNotContainKey("order_id");
    }

    @Test
    void toMap_handlesNullValues() throws Exception {
        ResultSetMetaData meta = mock(ResultSetMetaData.class);
        when(meta.getColumnCount()).thenReturn(2);
        when(meta.getColumnLabel(1)).thenReturn("id");
        when(meta.getColumnLabel(2)).thenReturn("value");

        ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(meta);
        when(rs.getObject(1)).thenReturn("123");
        when(rs.getObject(2)).thenReturn(null);

        Map<String, Object> row = ResultSetConverter.toMap(rs);

        assertThat(row).hasSize(2)
                       .containsEntry("id", "123")
                       .containsEntry("value", null);
    }

    @Test
    void toMap_preservesInsertionOrder() throws Exception {
        ResultSetMetaData meta = mock(ResultSetMetaData.class);
        when(meta.getColumnCount()).thenReturn(3);
        when(meta.getColumnLabel(1)).thenReturn("c");
        when(meta.getColumnLabel(2)).thenReturn("a");
        when(meta.getColumnLabel(3)).thenReturn("b");

        ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(meta);
        when(rs.getObject(1)).thenReturn(1);
        when(rs.getObject(2)).thenReturn(2);
        when(rs.getObject(3)).thenReturn(3);

        Map<String, Object> row = ResultSetConverter.toMap(rs);

        assertThat(row.keySet()).containsExactly("c", "a", "b");
    }

    @Test
    void toMap_emptyResultSet() throws Exception {
        ResultSetMetaData meta = mock(ResultSetMetaData.class);
        when(meta.getColumnCount()).thenReturn(0);

        ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(meta);

        Map<String, Object> row = ResultSetConverter.toMap(rs);

        assertThat(row).isEmpty();
    }
}
