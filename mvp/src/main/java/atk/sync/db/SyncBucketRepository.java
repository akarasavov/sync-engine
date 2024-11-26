package atk.sync.db;

import atk.sync.model.Operation;
import atk.sync.model.SyncRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static atk.sync.model.SyncRule.*;

public class SyncBucketRepository {
    private Logger logger = LoggerFactory.getLogger(SyncBucketRepository.class);
    private final SyncBucketName tableName;
    private final String jdbcPath;

    public SyncBucketRepository(SyncBucketName tableName, Path pathToDb) {
        this.tableName = tableName;
        this.jdbcPath = "jdbc:sqlite:" + pathToDb;
    }

    private SQLiteConfig readOnlyConfig() {
        var readOnlyConfig = new SQLiteConfig();
        readOnlyConfig.setReadOnly(true);
        return readOnlyConfig;
    }

    public String tableName() {
        return tableName.name();
    }

    public List<Operation> getAllOperations() {
        return query("SELECT * FROM " + tableName);
    }

    public List<Operation> getAllOperationsFrom(Instant instant) {
        return query("SELECT * FROM " + tableName + " where timestamp >" + instant);
    }

    private List<Operation> query(String sqlStatement) {
        List<Operation> result = new ArrayList<>();
        try (Connection connection = readOnlyConfig().createConnection(jdbcPath);
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

    private static Operation parseOperation(ResultSet resultSet) throws SQLException {
        var metaData = resultSet.getMetaData();
        var columnCount = metaData.getColumnCount();
        var builder = new Operation.Builder();
        for (int i = 1; i <= columnCount; i++) {
            var columnName = metaData.getColumnName(i);
            if (columnName.equals("id")) {
                continue;
            }
            var columnValue = resultSet.getObject(i);
            ;
            if (columnValue == null) {
                continue;
            }
            switch (columnName) {
                case "operation" -> builder.setType(Operation.Type.valueOf(columnValue.toString()));
                case "row_id" -> builder.setRowId((Integer) columnValue);
                case "table_name" -> builder.setTableName(columnValue.toString());
                case "timestamp" -> builder.setExecutedAt(Instant.ofEpochSecond((Integer) columnValue));
                default -> builder.addJsonParameter(columnName, columnValue);
            }
        }
        return builder.build();
    }
}
