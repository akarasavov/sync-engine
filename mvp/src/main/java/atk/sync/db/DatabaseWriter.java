package atk.sync.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static atk.sync.model.Models.SqlStatement;

public class DatabaseWriter {

    private final String jdbcPath;
    private final Logger logger = LoggerFactory.getLogger(DatabaseWriter.class);

    public DatabaseWriter(String jdbcPath) {
        this.jdbcPath = jdbcPath;
    }

//    public boolean applyOperations(List<Operation> operations, TableMetaData tableMetaData) {
//        try (Connection conn = DriverManager.getConnection(jdbcPath)) {
//            conn.setAutoCommit(true);
//            execute(conn, sqlStatements);
//        } catch (SQLException e) {
//            logger.debug("Wasn't able to execute {}", sqlStatements);
//            return false;
//        }
//        return true;
//    }

//    private List<SqlStatement> writeToOperationsTable(List<Operation> operations, TableMetaData tableMetaData) {
//        var pattern = "INSERT INTO %s (%s,%s,%s,%s,%s) VALUES (%s,%s,%s,%s,%s)";
//        return operations.stream().map(o -> {
//            var keySet = o.parameters().keySet();
//            var otherColumnNames = keySet.stream().collect(Collectors.joining(","));
//            var valueList = keySet.stream().map(k -> {
//                var value = o.parameters().get(k);
//                var type = tableMetaData.columnTypes().get(k);
//                //TODO this logic needs to be abstracted. THere is duplication
//                if (type == JDBCType.VARCHAR) {
//                    return "'" + value.getAsString() + "'";
//                } else if (type == JDBCType.INTEGER) {
//                    return value.getAsString();
//                } else {
//                    throw new IllegalStateException("Not supported type");
//                }
//            }).collect(Collectors.joining(","));
//            var insertStatement = String.format(pattern, TABLE_NAME_COLUMN, OPERATION_COLUMN, ROW_ID_COLUMN, TIMESTAMP_NAME_COLUMN, otherColumnNames,
//                    "'" + o.tableName() + "'", "'" + o.type().name() + "'", o.rowId(), o.getExecutedAt().toEpochMilli(), valueList);
//            return new SqlStatement(insertStatement);
//        }).toList();
//    }

    private void execute(Connection conn, List<SqlStatement> sqlStatements) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            for (SqlStatement sqlStatement : sqlStatements) {
                statement.execute(sqlStatement.value);
            }
        } catch (SQLException e) {
            conn.rollback();
        }
    }

}
