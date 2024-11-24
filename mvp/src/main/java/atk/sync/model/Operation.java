package atk.sync.model;

import com.google.gson.JsonObject;

import java.time.Instant;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Operation {
    private final Type type;
    //TODO the id should be UUID or other unique type;
    private Integer rowId;
    private final String tableName;
    private final JsonObject parameters;
    private final Instant executedAt;

    public Operation(Type type, String tableName, Integer rowId, JsonObject parameters, Instant executedAt) {
        this.type = requireNonNull(type);
        this.rowId = requireNonNull(rowId);
        this.tableName = requireNonNull(tableName);
        this.parameters = requireNonNull(parameters);
        this.executedAt = requireNonNull(executedAt);
    }

    public Integer rowId() {
        return rowId;
    }

    public Type type() {
        return type;
    }

    public String tableName() {
        return tableName;
    }

    public JsonObject parameters() {
        return parameters;
    }

    public enum Type {
        PUT, PATCH, DELETE, IGNORE
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Operation operation = (Operation) o;
        return type == operation.type && Objects.equals(tableName, operation.tableName) && Objects.equals(parameters, operation.parameters) && Objects.equals(executedAt, operation.executedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, rowId, tableName, parameters, executedAt);
    }

    @Override
    public String toString() {
        return "Operation{" +
                "type=" + type +
                ", rowId=" + rowId +
                ", tableName='" + tableName + '\'' +
                ", parameters=" + parameters +
                ", executedAt=" + executedAt +
                '}';
    }

    public static class Builder {
        private Type type;
        private Integer rowId;
        private String tableName;
        private JsonObject paremeters = new JsonObject();
        private Instant executedAt;

        public Builder setType(Type type) {
            this.type = type;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder addJsonParameter(String key, Object value) {
            this.paremeters.addProperty(key, value.toString());
            return this;
        }

        public Builder setExecutedAt(Instant executedAt) {
            this.executedAt = executedAt;
            return this;
        }

        public Builder setRowId(Integer rowId) {
            this.rowId = rowId;
            return this;
        }

        public Operation build() {
            return new Operation(type, tableName, rowId, paremeters, executedAt);
        }

    }
}
