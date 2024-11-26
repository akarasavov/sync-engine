package atk.sync.model;

import atk.sync.agent.BaseTest;
import atk.sync.util.MetaTableReader;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static atk.sync.model.Models.ConflictKey;
import static atk.sync.model.Models.ConflictResolutionStrategy;
import static atk.sync.model.Models.SqlStatement;
import static atk.sync.model.Models.SyncBucketName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SyncRuleTest extends BaseTest {

    @Test
    void syncRuleShouldBeAbleToGenerateCreateDBSqlStatement() throws SQLException {
        var syncRule = new SyncRule(new SyncBucketName("snippet_sync_bucket1"),
                Models.ConflictResolutionStrategy.LWW,
                ConflictKey.idConflictKey(),
                new SqlStatement("SELECT * FROM snippets"));
        var tableMetadata = MetaTableReader.getTableMetadata(jdbcPath, syncRule.selectStatement);

        var expected = """
                CREATE TABLE snippet_sync_bucket1 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,
                table_name TEXT NOT NULL,
                row_id INTEGER NOT NULL,
                name VARCHAR,
                snippet_text VARCHAR,
                timestamp INTEGER DEFAULT (unixepoch()));""";
        assertEquals(new SqlStatement(expected), syncRule.generateCreateTableStatement(tableMetadata));
    }

    @Test
    void syncRuleShouldBeAbleToGenerateCreateDBSqlStatement1() throws SQLException {
        var syncRule = new SyncRule(new SyncBucketName("snippet_sync_bucket"),
                ConflictResolutionStrategy.LWW,
                ConflictKey.idConflictKey(),
                new SqlStatement("SELECT id,name FROM snippets"));
        var tableMetadata = MetaTableReader.getTableMetadata(jdbcPath, syncRule.selectStatement);

        var expected = """
                CREATE TABLE snippet_sync_bucket (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,
                table_name TEXT NOT NULL,
                row_id INTEGER NOT NULL,
                name VARCHAR,
                timestamp INTEGER DEFAULT (unixepoch()));""";
        assertEquals(new SqlStatement(expected), syncRule.generateCreateTableStatement(tableMetadata));
    }

    @Test
    void syncRuleShouldFailToGenerateCreateDbIfIdIsNotSelected() throws SQLException {
        var syncRule = new SyncRule(new SyncBucketName("snippet_sync_bucket1"),
                ConflictResolutionStrategy.LWW,
                ConflictKey.idConflictKey(),
                new SqlStatement("SELECT name FROM snippets"));
        var tableMetadata = MetaTableReader.getTableMetadata(jdbcPath, syncRule.selectStatement);

        assertThrows(IllegalStateException.class, () -> syncRule.generateCreateTableStatement(tableMetadata));
    }

    @Test
    void syncRuleShouldGenerateInsertTriggerStatement() throws SQLException {
        var syncRule = new SyncRule(new SyncBucketName("snippet_sync_bucket"),
                ConflictResolutionStrategy.LWW,
                ConflictKey.idConflictKey(),
                new SqlStatement("SELECT name FROM snippets"));
        var tableMetadata = MetaTableReader.getTableMetadata(jdbcPath, syncRule.selectStatement);
        var expected = """
                CREATE TRIGGER snippet_sync_bucket_insert
                AFTER INSERT ON snippets
                BEGIN
                    INSERT INTO snippet_sync_bucket (operation, table_name, row_id, name)
                    VALUES ('PUT', 'snippets', NEW.id, NEW.name);
                END;""";
        assertEquals(new SqlStatement(expected), syncRule.generateInsertTriggerStatement(tableMetadata));
    }

    @Test
    void syncRuleShouldGenerateUpdateTriggerStatement() throws SQLException {
        var syncRule = new SyncRule(new SyncBucketName("temp_bucket"),
                ConflictResolutionStrategy.LWW,
                ConflictKey.idConflictKey(),
                new SqlStatement("SELECT * FROM snippets"));
        var tableMetadata = MetaTableReader.getTableMetadata(jdbcPath, syncRule.selectStatement);

        var expected = """
                CREATE TRIGGER temp_bucket_update
                AFTER UPDATE ON snippets
                BEGIN
                    INSERT INTO temp_bucket (operation, table_name, row_id, name,snippet_text)
                    VALUES ('PATCH', 'snippets', NEW.id, NEW.name,NEW.snippet_text);
                END;""";
        assertEquals(new SqlStatement(expected), syncRule.generateUpdateTriggerStatement(tableMetadata));
    }

    @Test
    void syncRuleShouldGenerateDeleteTriggerStatement() throws SQLException {
        var syncRule = new SyncRule(new SyncBucketName("snippet_sync_bucket"),
                ConflictResolutionStrategy.LWW,
                ConflictKey.idConflictKey(),
                new SqlStatement("SELECT * FROM snippets"));
        var tableMetadata = MetaTableReader.getTableMetadata(jdbcPath, syncRule.selectStatement);
        var expected = """
                CREATE TRIGGER snippet_sync_bucket_delete
                AFTER DELETE ON snippets
                BEGIN
                    INSERT INTO snippet_sync_bucket (operation, table_name, row_id)
                    VALUES ('DELETE', 'snippets', OLD.id);
                END;""";
        assertEquals(new SqlStatement(expected), syncRule.generateDeleteTriggerStatement(tableMetadata));
    }
}