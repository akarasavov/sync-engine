package atk.sync.model;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public class Models {

    public static class SyncFeatureTableName extends StringModel{

        public SyncFeatureTableName(String value) {
            super(value);
        }
    }

    public static class SyncBucketName extends StringModel {
        public SyncBucketName(String name) {
            super(name);
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

    public static class SqlStatement extends StringModel {
        public SqlStatement(String sqlStatement) {
            super(sqlStatement);
        }
    }

    public static class JDBCPath extends StringModel {

        public JDBCPath(Path pathToDb) {
            super("jdbc:sqlite:" + pathToDb);
        }
    }

    private static abstract class StringModel {
        public final String value;

        public StringModel(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StringModel that = (StringModel) o;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(value);
        }
    }

    public record Pair<V1, V2>(V1 first, V2 second) {
        // intentionally empty
    }
}
