package atk.sync.util;

import atk.sync.model.SyncRule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static atk.sync.model.Models.JDBCPath;
import static atk.sync.model.Models.Pair;
import static atk.sync.model.Models.SqlStatement;
import static atk.sync.model.Models.SyncBucketName;
import static atk.sync.model.Models.SyncFeatureTableName;

public class MetaTableReader {

    public static class TableMetaData {
        private final SyncFeatureTableName tableName;
        private final Map<String, JDBCType> columnTypes;

        public TableMetaData(SyncFeatureTableName tableName, Map<String, JDBCType> columnTypes) {
            this.tableName = tableName;
            this.columnTypes = columnTypes;
        }

        public SyncFeatureTableName tableName() {
            return tableName;
        }

        public List<String> columnNameList() {
            return columnTypes
                    .keySet()
                    .stream()
                    .filter(v -> !v.equals("id") && !v.equals("timestamp"))
                    .sorted()
                    .toList();
        }

        public Map<String, JDBCType> columnTypes() {
            return columnTypes;
        }

        public Map<String, JDBCType> getColumnTypesWithoutId() {
            return columnTypes.entrySet()
                    .stream()
                    .filter(e -> !e.getKey().equals("id"))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    public static Map<SyncBucketName, Pair<TableMetaData, SyncRule>> getTableMetadata(JDBCPath path, List<SyncRule> syncRules) throws SQLException {
        Map<SyncBucketName, Pair<TableMetaData, SyncRule>> result = new HashMap<>();
        try (Connection conn = DriverManager.getConnection(path.value);
             Statement stmt = conn.createStatement()) {
            for (SyncRule syncRule : syncRules) {
                var resultSet = stmt.executeQuery(syncRule.selectStatement.value);

                var metaData = resultSet.getMetaData();
                result.put(syncRule.bucketName, new Pair<>(convertTo(metaData), syncRule));
            }
        }
        return result;
    }

    public static TableMetaData getTableMetadata(JDBCPath path, SqlStatement selectStatement) throws SQLException {
        try (Connection conn = DriverManager.getConnection(path.value);
             Statement stmt = conn.createStatement()) {
            var resultSet = stmt.executeQuery(selectStatement.value);

            var metaData = resultSet.getMetaData();
            return convertTo(metaData);
        }
    }

    private static TableMetaData convertTo(ResultSetMetaData metaData) throws SQLException {
        Map<String, JDBCType> columnTypes = new HashMap<>();
        var tableName = metaData.getTableName(1);
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            var columnType = metaData.getColumnType(i);
            var columnName = metaData.getColumnName(i);
            columnTypes.put(columnName, JDBCType.valueOf(columnType));
        }
        return new TableMetaData(new SyncFeatureTableName(tableName), columnTypes);
    }
}
