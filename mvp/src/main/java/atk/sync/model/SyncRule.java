package atk.sync.model;

import java.sql.SQLException;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static atk.sync.model.Models.ConflictKey;
import static atk.sync.model.Models.ConflictResolutionStrategy;
import static atk.sync.model.Models.SqlStatement;
import static atk.sync.model.Models.SyncBucketName;
import static atk.sync.util.MetaTableReader.TableMetaData;

public class SyncRule {
    public final SyncBucketName bucketName;
    public final ConflictResolutionStrategy conflictResolutionStrategy;
    public final ConflictKey conflictKey;
    public final SqlStatement selectStatement;
    private final String insertTriggerName;
    private final String updateTriggerName;
    private final String deleteTriggerName;

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

    public SqlStatement generateCreateTableStatement(TableMetaData tableMetaData) throws SQLException {
        if (tableMetaData.columnTypes().get("id") == null) {
            throw new IllegalStateException(this + " doesn't select id");
        }
        StringJoiner otherParameters = new StringJoiner("");
        tableMetaData.columnNameList().forEach(column -> {
            var type = tableMetaData.columnTypes().get(column);
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

    public SqlStatement generateInsertTriggerStatement(TableMetaData tableMetaData) {
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

    public SqlStatement generateUpdateTriggerStatement(TableMetaData tableMetaData) {
        var columnNames = tableMetaData.columnNameList();
        var columnNameList = String.join(",", columnNames);
        var valueList = columnNames.stream().map(v -> "NEW." + v).collect(Collectors.joining(","));
        var updateTrigger = "CREATE TRIGGER " + updateTriggerName + "\n" +
                "AFTER UPDATE ON " + tableMetaData.tableName() + "\n" +
                "BEGIN\n" +
                "    INSERT INTO " + bucketName + " (operation, table_name, row_id, " + columnNameList + ")\n" +
                "    VALUES ('PATCH', '" + tableMetaData.tableName() + "', NEW.id, " + valueList + ");\n" +
                "END;";
        return new SqlStatement(updateTrigger);
    }

    public SqlStatement generateDeleteTriggerStatement(TableMetaData tableMetaData) {
        var deleteTrigger = "CREATE TRIGGER " + deleteTriggerName + "\n" +
                "AFTER DELETE ON " + tableMetaData.tableName() + "\n" +
                "BEGIN\n" +
                "    INSERT INTO " + bucketName + " (operation, table_name, row_id)\n" +
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
