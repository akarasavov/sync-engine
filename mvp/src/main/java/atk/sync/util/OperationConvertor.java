package atk.sync.util;

import atk.sync.model.Operation;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static atk.sync.model.Models.SqlStatement;
import static atk.sync.model.Operation.SyncTableName;

public class OperationConvertor {

    private static final String INSERT_PATTERN = "INSERT INTO %s %s VALUES %s;";
    private static final String UPDATE_PATTERN = "UPDATE %s SET %s WHERE id=%s;";
    private static final String DELETE_PATTERN = "DELETE FROM %s WHERE id=%s";

    public static List<SqlStatement> toSqlStatement(List<Operation> operations, Map<SyncTableName, Map<String, Integer>> tableColumnTypes) {
        List<SqlStatement> sqlStatements = new ArrayList<>();
        operations.forEach(o -> {
            var columnTypes = tableColumnTypes.get(o.tableName());
            switch (o.type()) {
                case PUT -> {
                    sqlStatements.add(toInsertAgainstSyncTables(o, columnTypes));
                    break;
                }
                case PATCH -> {
                    sqlStatements.add(toUpdateAgainstSyncTables(o, columnTypes));
                    break;
                }
                case DELETE -> {
                    sqlStatements.add(toDeleteAgainstSyncTables(o));
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

    private static SqlStatement toInsertAgainstSyncTables(Operation operation, Map<String, Integer> columnTypes) {
        StringJoiner columnList = new StringJoiner(",", "(", ")");
        StringJoiner valueList = new StringJoiner(",", "(", ")");
        columnList.add("id");
        valueList.add(operation.rowId().toString());
        var parameters = getParameters(operation.parameters(), columnTypes);
        parameters.forEach((key, value) -> {
            columnList.add(key);
            valueList.add(value);
        });
        return new SqlStatement(String.format(INSERT_PATTERN, operation.tableName(), columnList, valueList));
    }

    private static SqlStatement toUpdateAgainstSyncTables(Operation operation, Map<String, Integer> columnTypes) {
        StringJoiner parameterList = new StringJoiner(",");
        var parameters = getParameters(operation.parameters(), columnTypes);
        parameters.forEach((key, value) -> parameterList.add(key + "=" + value));
        return new SqlStatement(String.format(UPDATE_PATTERN, operation.tableName(), parameterList, operation.rowId()));
    }

    private static SqlStatement toDeleteAgainstSyncTables(Operation operation) {
        return new SqlStatement(String.format(DELETE_PATTERN, operation.tableName(), operation.rowId()));
    }

    private static Map<String, String> getParameters(JsonObject operationParameters, Map<String, Integer> columnTypes) {
        Map<String, String> result = new HashMap<>();
        operationParameters.keySet().forEach(k -> {
            var value = getValue(operationParameters.get(k), k, columnTypes);
            result.put(k, value);
        });
        return result;
    }

    private static String getValue(JsonElement value, String key, Map<String, Integer> columnTypes) {
        var type = columnTypes.get(key);
        if (type == Types.INTEGER) {
            return String.valueOf(value.getAsInt());
        } else if (type == Types.VARCHAR) {
            return "'" + value.getAsString() + "'";
        } else {
            throw new IllegalStateException("Not supported");
        }
    }

}
