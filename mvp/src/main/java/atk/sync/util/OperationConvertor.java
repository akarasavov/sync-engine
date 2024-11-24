package atk.sync.util;

import atk.sync.model.Operation;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static atk.sync.util.ExceptionUtils.*;

public class OperationConvertor {

    private static final String INSERT_PATTERN = "INSERT INTO %s %s VALUES %s;";
    private static final String UPDATE_PATTERN = "UPDATE %s SET %s WHERE id=%s;";
    private static final String DELETE_PATTERN = "DELETE FROM %s WHERE id=%s";

    public static List<String> toSqlStatement(List<Operation> operations, Map<String, Map<String, Integer>> tableColumnTypes) {
        List<String> sqlStatements = new ArrayList<>();
        operations.forEach(o -> {
            var columnTypes = tableColumnTypes.get(o.tableName());
            switch (o.type()) {
                case PUT -> {
                    sqlStatements.add(toInsert(o, columnTypes));
                    break;
                }
                case PATCH -> {
                    sqlStatements.add(toUpdate(o, columnTypes));
                    break;
                }
                case DELETE -> {
                    sqlStatements.add(toDelete(o));
                    break;
                }
                case IGNORE -> {
                    //ignore operation is ignored by operation convertor
                    break;
                }
            }
        });
        return sqlStatements;
    }

    private static String toInsert(Operation operation, Map<String, Integer> columnTypes) {
        StringJoiner columnList = new StringJoiner(",", "(", ")");
        StringJoiner valueList = new StringJoiner(",", "(", ")");
        columnList.add("id");
        valueList.add(operation.rowId().toString());
        var parameters = getParameters(operation.parameters(), columnTypes);
        parameters.entrySet().forEach(e -> {
            columnList.add(e.getKey());
            valueList.add(e.getValue());
        });
        return String.format(INSERT_PATTERN, operation.tableName(), columnList, valueList);
    }

    private static String toUpdate(Operation operation, Map<String, Integer> columnTypes) {
        StringJoiner parameterList = new StringJoiner(",");
        var parameters = getParameters(operation.parameters(), columnTypes);
        parameters.entrySet().forEach(e -> {
            parameterList.add(e.getKey() + "=" + e.getValue());
        });
        return String.format(UPDATE_PATTERN, operation.tableName(), parameterList, operation.rowId());
    }

    private static String toDelete(Operation operation) {
        return String.format(DELETE_PATTERN, operation.tableName(), operation.rowId());
    }

    private static Map<String, String> getParameters(JsonObject operationParameters, Map<String, Integer> columnTypes) {
        Map<String, String> result = new HashMap<>();
        operationParameters.keySet().forEach(k -> {
            var value = getValue(operationParameters, k, columnTypes);
            result.put(k, value);
        });
        return result;
    }

    private static String getValue(JsonObject operationParameters, String key, Map<String, Integer> columnTypes) {
        var type = columnTypes.get(key);
        var value = operationParameters.get(key);
        if (type == Types.INTEGER) {
            return String.valueOf(value.getAsInt());
        } else if (type == Types.VARCHAR) {
            return "'" + value.getAsString() + "'";
        } else {
            throw new IllegalStateException("Not supported");
        }
    }

    public static Map<String, Map<String, Integer>> getTableColumnTypes(Path pathToDb, List<String> tableNames) {
        String jdbcPath = "jdbc:sqlite:" + pathToDb;
        return wrapToRuntimeException(() -> {
            Map<String, Map<String, Integer>> result = new HashMap<>();
            try (Connection connection = DriverManager.getConnection(jdbcPath); Statement statement = connection.createStatement()) {
                tableNames.forEach(t -> {
                    wrapToRuntimeException(() -> {
                        var resultSet = statement.executeQuery("select * from " + t);
                        var metaData = resultSet.getMetaData();
                        for (int i = 1; i <= metaData.getColumnCount(); i++) {
                            var columnType = metaData.getColumnType(i);
                            var columnName = metaData.getColumnName(i);
                            var value = result.getOrDefault(t, new HashMap<>());
                            value.put(columnName, columnType);
                            result.put(t, value);
                        }
                    });

                });
            }
            return result;
        });
    }

}
