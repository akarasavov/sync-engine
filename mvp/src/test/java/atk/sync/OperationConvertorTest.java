package atk.sync;

import atk.sync.util.OperationConvertor;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static atk.sync.model.Operation.*;
import static atk.sync.model.SyncRule.SqlStatement;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OperationConvertorTest {

    @Test
    void shouldConvertPutToInsert() {
        var operation = new Builder().setType(Type.PUT)
                .setTableName(new SyncTableName("table1"))
                .setExecutedAt(Instant.now())
                .setRowId(1)
                .addJsonParameter("a1", 1)
                .addJsonParameter("a2", "str1").build();

        assertEquals(List.of(new SqlStatement("INSERT INTO table1 (id,a1,a2) VALUES (1,1,'str1');")),
                OperationConvertor.toSqlStatement(List.of(operation),
                        Map.of(new SyncTableName("table1"), Map.of("a1", Types.INTEGER, "a2", Types.VARCHAR))));
    }

    @Test
    void shouldConvertPatchToUpdate() {
        var operation = new Builder().setType(Type.PATCH)
                .setTableName(new SyncTableName("table1"))
                .setExecutedAt(Instant.now())
                .setRowId(1)
                .addJsonParameter("a1", 1)
                .addJsonParameter("a2", "str1").build();

        assertEquals(List.of(new SqlStatement("UPDATE table1 SET a1=1,a2='str1' WHERE id=1;")),
                OperationConvertor.toSqlStatement(List.of(operation),
                        Map.of(new SyncTableName("table1"), Map.of("a1", Types.INTEGER, "a2", Types.VARCHAR))));
    }

    @Test
    void shouldConvertDeleteOperation() {
        var operation = new Builder().setType(Type.DELETE)
                .setTableName(new SyncTableName("table1"))
                .setExecutedAt(Instant.now())
                .setRowId(1).build();
        var sqlStatement = OperationConvertor.toSqlStatement(List.of(operation), Map.of());
        assertEquals(List.of(new SqlStatement("DELETE FROM table1 WHERE id=1")), sqlStatement);
    }

}