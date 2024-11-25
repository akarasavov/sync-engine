package atk.sync.util;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MetaTableReader {

    private final String jdbcPath;

    public MetaTableReader(String jdbcPath) {
        this.jdbcPath = jdbcPath;
    }

    public static class TableMetaData {

        private final String tableName;
        private final Map<String, JDBCType> columnTypes;

        public TableMetaData(String tableName, Map<String, JDBCType> columnTypes) {
            this.tableName = tableName;
            this.columnTypes = columnTypes;
        }

        public String tableName() {
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

    public static TableMetaData convertTo(ResultSetMetaData metaData) throws SQLException {
        Map<String, JDBCType> columnTypes = new HashMap<>();
        var tableName = metaData.getTableName(1);
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            var columnType = metaData.getColumnType(i);
            var columnName = metaData.getColumnName(i);
            columnTypes.put(columnName, JDBCType.valueOf(columnType));
        }
        return new TableMetaData(tableName, columnTypes);
    }
}
