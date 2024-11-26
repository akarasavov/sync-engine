package atk.sync.model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static atk.sync.util.MetaTableReader.TableMetaData;
import static atk.sync.util.MetaTableReader.convertTo;

public class SyncRule {
    public final SyncBucketName bucketName;
    public final ConflictResolutionStrategy conflictResolutionStrategy;
    public final ConflictKey conflictKey;
    public final SqlStatement selectStatement;
    private final String insertTriggerName;
    private final String updateTriggerName;
    private final String deleteTriggerName;
    private Optional<TableMetaData> selectStatementMetadata = Optional.empty();

    public SyncRule(SyncBucketName bucketName,
                    ConflictResolutionStrategy conflictResolutionStrategy,
                    ConflictKey conflictKey,
                    SqlStatement selectStatement) {
        this.bucketName = bucketName;
        this.conflictResolutionStrategy = conflictResolutionStrategy;
        this.conflictKey = conflictKey;
        this.selectStatement = selectStatement;
        this.insertTriggerName = bucketName + "_insert";
        this.updateTriggerName = bucketName + "_update";
        this.deleteTriggerName = bucketName + "_delete";
    }

    public record SyncBucketName(String name) {
        @Override
        public String toString() {
            return name;
        }
    }

    public record ConflictKey(List<String> columnNames) {
        public ConflictKey(String column) {
            this(List.of(column));
        }

        public static ConflictKey idConflictKey() {
            return new ConflictKey("id");
        }
    }

    public enum ConflictResolutionStrategy {
        LWW
    }

    public record SqlStatement(String sqlStatement) {
    }

    private TableMetaData getTableMetadata(String dbUrl) throws SQLException {
        if (selectStatementMetadata.isEmpty()) {
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 Statement stmt = conn.createStatement()) {
                var resultSet = stmt.executeQuery(selectStatement.sqlStatement);

                var metaData = resultSet.getMetaData();
                var tableMetaData = convertTo(metaData);
                this.selectStatementMetadata = Optional.of(tableMetaData);
            }
        }
        return selectStatementMetadata.get();
    }

    public SqlStatement generateCreateTableStatement(String dbUrl) throws SQLException {
        var tableMetadata = getTableMetadata(dbUrl);
        if (tableMetadata.columnTypes().get("id") == null) {
            throw new IllegalStateException(this + " doesn't select id");
        }
        StringJoiner otherParameters = new StringJoiner("");
        tableMetadata.columnNameList().forEach(column -> {
            var type = tableMetadata.columnTypes().get(column);
            otherParameters.add(column + " " + type + ",\n");
        });
        StringBuilder createTableStatement = new StringBuilder(
                "CREATE TABLE " + bucketName + " (\n" +
                        "id INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
                        "operation TEXT NOT NULL,\n" +
                        "table_name TEXT NOT NULL,\n" +
                        "row_id INTEGER NOT NULL,\n");
        createTableStatement.append(otherParameters)
                .append("timestamp INTEGER DEFAULT (unixepoch()));");

        return new SqlStatement(createTableStatement.toString());
    }

    public SqlStatement generateInsertTriggerStatement(String dbUrl) throws SQLException {
        var tableMetaData = getTableMetadata(dbUrl);
        var columnNames = tableMetaData.getColumnTypesWithoutId().keySet();
        var columnNameList = String.join(",", columnNames);
        var valueList = columnNames.stream().map(v -> "NEW." + v).collect(Collectors.joining(","));
        var createTrigger = "CREATE TRIGGER " + insertTriggerName + "\n" +
                "AFTER INSERT ON " + tableMetaData.tableName() + "\n" +
                "BEGIN\n" +
                "    INSERT INTO " + bucketName + " (operation, table_name, row_id, " + columnNameList + ")\n" +
                "    VALUES ('PUT', '" + tableMetaData.tableName() + "', NEW.id, " + valueList + ");\n" +
                "END;";
        return new SqlStatement(createTrigger);
    }

    public SqlStatement generateUpdateTriggerStatement(String dbUrl) throws SQLException {
        var tableMetaData = getTableMetadata(dbUrl);
        var columnNames = tableMetaData.columnNameList();
        var columnNameList = String.join(",", columnNames);
        var valueList = columnNames.stream().map(v -> "NEW." + v).collect(Collectors.joining(","));
        var updateTrigger = "CREATE TRIGGER " + updateTriggerName + "\n" +
                "AFTER UPDATE ON " + tableMetaData.tableName() + "\n" +
                "BEGIN\n" +
                "    INSERT INTO snippet_sync_bucket (operation, table_name, row_id, " + columnNameList + ")\n" +
                "    VALUES ('PATCH', '" + tableMetaData.tableName() + "', NEW.id, " + valueList + ");\n" +
                "END;";
        return new SqlStatement(updateTrigger);
    }

    public SqlStatement generateDeleteTriggerStatement(String dbUrl) throws SQLException {
        var tableMetaData = getTableMetadata(dbUrl);
        var deleteTrigger = "CREATE TRIGGER " + deleteTriggerName + "\n" +
                "AFTER DELETE ON " + tableMetaData.tableName() + "\n" +
                "BEGIN\n" +
                "    INSERT INTO snippet_sync_bucket (operation, table_name, row_id)\n" +
                "    VALUES ('DELETE', '" + tableMetaData.tableName() + "', OLD.id);\n" +
                "END;";
        return new SqlStatement(deleteTrigger);
    }

    public List<SqlStatement> dropCreatedTriggers() {
        var pattern = "DROP TRIGGER IF EXISTS %s;";
        return List.of(insertTriggerName, updateTriggerName, deleteTriggerName)
                .stream()
                .map(v -> String.format(pattern, v))
                .map(SqlStatement::new)
                .toList();
    }
}
