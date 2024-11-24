package atk.sync.db;

import atk.sync.model.Operation;
import atk.sync.util.ExceptionUtils;
import org.sqlite.SQLiteConfig;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static atk.sync.util.ExceptionUtils.wrapToRuntimeException;

public class SyncBucketRepository {

    private final String tableName;
    private final String jdbcPath;

    public SyncBucketRepository(String tableName, Path pathToDb) {
        this.tableName = tableName;
        this.jdbcPath = "jdbc:sqlite:" + pathToDb;
    }

    private SQLiteConfig readOnlyConfig() {
        var readOnlyConfig = new SQLiteConfig();
        readOnlyConfig.setReadOnly(true);
        return readOnlyConfig;
    }

    public String tableName() {
        return tableName;
    }

    public List<Operation> getAllOperations() {
        return query("SELECT * FROM " + tableName);
    }

    public List<Operation> getAllOperationsFrom(Instant instant) {
        return query("SELECT * FROM " + tableName + " where timestamp >" + instant);
    }

    private List<Operation> query(String sqlStatement) {
        return wrapToRuntimeException(() -> {
            List<Operation> result = new ArrayList<>();
            try (Connection connection = wrapToRuntimeException(() -> readOnlyConfig().createConnection(jdbcPath));
                 Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(10);
                var resultSet = statement.executeQuery(sqlStatement);
                while (resultSet.next()) {
                    result.add(parseOperation(resultSet));
                }
            }
            return result;
        });
    }

    private static Operation parseOperation(ResultSet resultSet) throws SQLException {
        var metaData = resultSet.getMetaData();
        var columnCount = metaData.getColumnCount();
        var builder = new Operation.Builder();
        for (int i = 1; i <= columnCount; i++) {
            var columnName = metaData.getColumnName(i);
            if (columnName.equals("id")) {
                continue;
            }
            var columnType = metaData.getColumnType(i);
            var columnValue = getValue(columnType, resultSet, i);
            if (columnValue == null) {
                continue;
            }
            if (columnName.equals("operation")) {
                builder.setType(Operation.Type.valueOf(columnValue.toString()));
            } else if (columnName.equals("row_id")) {
                builder.setRowId((Integer) columnValue);
            } else if (columnName.equals("table_name")) {
                builder.setTableName(columnValue.toString());
            } else if (columnName.equals("timestamp")) {
                builder.setExecutedAt(Instant.ofEpochSecond((Integer) columnValue));
            } else {
                builder.addJsonParameter(columnName, columnValue);
            }
        }
        return builder.build();
    }

    private static Object getValue(int columnType, ResultSet resultSet, int i) throws SQLException {
        return switch (columnType) {
            case Types.INTEGER -> resultSet.getInt(i);
            case Types.VARCHAR -> resultSet.getString(i);
            default -> throw new IllegalStateException("Not supported type " + columnType);
        };
    }
}
