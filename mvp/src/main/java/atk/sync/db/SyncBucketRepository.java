package atk.sync.db;

import atk.sync.model.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static atk.sync.model.Models.JDBCPath;
import static atk.sync.model.Models.SyncBucketName;
import static atk.sync.model.Operation.Builder;
import static atk.sync.model.Operation.SyncTableName;
import static atk.sync.model.Operation.Type;

//TODO - hold connection with database, do not instantiate each time
public class SyncBucketRepository {
    private final Logger logger = LoggerFactory.getLogger(SyncBucketRepository.class);
    private final SyncBucketName tableName;
    private final JDBCPath jdbcPath;
    private static final String OPERATION_COLUMN = "operation";
    private static final String ROW_ID_COLUMN = "row_id";
    private static final String TABLE_NAME_COLUMN = "table_name";
    private static final String TIMESTAMP_NAME_COLUMN = "timestamp";

    public SyncBucketRepository(SyncBucketName tableName, JDBCPath jdbcPath) {
        this.tableName = tableName;
        this.jdbcPath = jdbcPath;
    }

    private SQLiteConfig readOnlyConfig() {
        var readOnlyConfig = new SQLiteConfig();
        readOnlyConfig.setReadOnly(true);
        return readOnlyConfig;
    }

    public SyncBucketName tableName() {
        return tableName;
    }

    public List<Operation> getAllOperations() {
        return query("SELECT * FROM " + tableName);
    }

    public List<Operation> getAllOperationsFrom(Instant instant) {
        return query("SELECT * FROM " + tableName + " where timestamp >" + instant);
    }

    private List<Operation> query(String sqlStatement) {
        List<Operation> result = new ArrayList<>();
        try (Connection connection = readOnlyConfig().createConnection(jdbcPath.value);
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(10);
            var resultSet = statement.executeQuery(sqlStatement);
            while (resultSet.next()) {
                result.add(parseOperation(resultSet));
            }
        } catch (SQLException e) {
            logger.warn("Wasn't able to execute query {}", sqlStatement);
        }
        return result;
    }

    private Operation parseOperation(ResultSet resultSet) throws SQLException {
        var metaData = resultSet.getMetaData();
        var columnCount = metaData.getColumnCount();
        var builder = new Builder();
        for (int i = 1; i <= columnCount; i++) {
            var columnName = metaData.getColumnName(i);
            if (columnName.equals("id")) {
                continue;
            }
            var columnValue = resultSet.getObject(i);
            if (columnValue == null) {
                continue;
            }
            switch (columnName) {
                case OPERATION_COLUMN -> builder.setType(Type.valueOf(columnValue.toString()));
                case ROW_ID_COLUMN -> builder.setRowId((Integer) columnValue);
                case TABLE_NAME_COLUMN -> builder.setTableName(new SyncTableName(columnValue.toString()));
                case TIMESTAMP_NAME_COLUMN -> builder.setExecutedAt(Instant.ofEpochSecond((Integer) columnValue));
                default -> builder.addJsonParameter(columnName, columnValue);
            }
        }
        return builder.build();
    }
}
