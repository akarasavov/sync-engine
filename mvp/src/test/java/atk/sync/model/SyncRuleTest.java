package atk.sync.model;

import atk.sync.agent.BaseTest;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static atk.sync.model.SyncRule.ConflictKey;
import static atk.sync.model.SyncRule.ConflictResolutionStrategy;
import static atk.sync.model.SyncRule.SqlStatement;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SyncRuleTest extends BaseTest {

    @Test
    void syncRuleShouldBeAbleToGenerateCreateDBSqlStatement() throws SQLException {
        var syncRule = new SyncRule("snippet_sync_bucket1",
                ConflictResolutionStrategy.LWW,
                ConflictKey.idConflictKey(),
                new SqlStatement("SELECT * FROM snippets"));

        var expected = "CREATE TABLE snippet_sync_bucket1 (\n" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
                "operation TEXT NOT NULL,\n" +
                "table_name TEXT NOT NULL,\n" +
                "row_id INTEGER NOT NULL,\n" +
                "name VARCHAR,\n" +
                "snippet_text VARCHAR,\n" +
                "timestamp INTEGER DEFAULT (unixepoch()));";
        assertEquals(expected, syncRule.generateCreateTableStatement(dbUrl).sqlStatement());
    }

    @Test
    void syncRuleShouldBeAbleToGenerateCreateDBSqlStatement1() throws SQLException {
        var syncRule = new SyncRule("snippet_sync_bucket",
                ConflictResolutionStrategy.LWW,
                ConflictKey.idConflictKey(),
                new SqlStatement("SELECT id,name FROM snippets"));

        var expected = """
                CREATE TABLE snippet_sync_bucket (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,
                table_name TEXT NOT NULL,
                row_id INTEGER NOT NULL,
                name VARCHAR,
                timestamp INTEGER DEFAULT (unixepoch()));""";
        assertEquals(expected, syncRule.generateCreateTableStatement(dbUrl).sqlStatement());
    }

    @Test
    void syncRuleShouldFailToGenerateCreateDbIfIdIsNotSelected() {
        var syncRule = new SyncRule("snippet_sync_bucket1",
                ConflictResolutionStrategy.LWW,
                ConflictKey.idConflictKey(),
                new SqlStatement("SELECT name FROM snippets"));

        assertThrows(IllegalStateException.class, () -> syncRule.generateCreateTableStatement(dbUrl));
    }

    @Test
    void syncRuleShouldGenerateInsertTriggerStatement() throws SQLException {
        var syncRule = new SyncRule("snippet_sync_bucket",
                ConflictResolutionStrategy.LWW,
                ConflictKey.idConflictKey(),
                new SqlStatement("SELECT name FROM snippets"));
        var expected = """
                CREATE TRIGGER snippet_sync_bucket_insert
                AFTER INSERT ON snippets
                BEGIN
                    INSERT INTO snippet_sync_bucket (operation, table_name, row_id, name)
                    VALUES ('PUT', 'snippets', NEW.id, NEW.name);
                END;""";
        assertEquals(new SqlStatement(expected), syncRule.generateInsertTriggerStatement(dbUrl));
    }

    @Test
    void syncRuleShouldGenerateUpdateTriggerStatement() throws SQLException {
        var syncRule = new SyncRule("snippet_sync_bucket",
                ConflictResolutionStrategy.LWW,
                ConflictKey.idConflictKey(),
                new SqlStatement("SELECT * FROM snippets"));

        var expected = """
                CREATE TRIGGER snippet_sync_bucket_update
                AFTER UPDATE ON snippets
                BEGIN
                    INSERT INTO snippet_sync_bucket (operation, table_name, row_id, name,snippet_text)
                    VALUES ('PATCH', 'snippets', NEW.id, NEW.name,NEW.snippet_text);
                END;""";
        assertEquals(new SqlStatement(expected), syncRule.generateUpdateTriggerStatement(dbUrl));
    }

    @Test
    void syncRuleShouldGenerateDeleteTriggerStatement() throws SQLException {
        var syncRule = new SyncRule("snippet_sync_bucket",
                ConflictResolutionStrategy.LWW,
                ConflictKey.idConflictKey(),
                new SqlStatement("SELECT * FROM snippets"));
        var expected = """
                CREATE TRIGGER snippet_sync_bucket_delete
                AFTER DELETE ON snippets
                BEGIN
                    INSERT INTO snippet_sync_bucket (operation, table_name, row_id)
                    VALUES ('DELETE', 'snippets', OLD.id);
                END;""";
        assertEquals(new SqlStatement(expected), syncRule.generateDeleteTriggerStatement(dbUrl));
    }

    /**
     * --!--
     * CREATE TABLE snippet_sync_bucket (
     *     id INTEGER PRIMARY KEY AUTOINCREMENT,
     *     operation TEXT NOT NULL,
     *     table_name TEXT NOT NULL,
     *     row_id INTEGER NOT NULL,
     *     name TEXT,
     *     snippet_text TEXT,
     *     timestamp INTEGER DEFAULT (unixepoch())
     * );
     * --!--
     * CREATE TRIGGER snippets_sync_insert
     * AFTER INSERT ON snippets
     * BEGIN
     *     INSERT INTO snippet_sync_bucket (operation, table_name, row_id, name, snippet_text)
     *     VALUES ('PUT', 'snippets', NEW.id, NEW.name, NEW.snippet_text);
     * END;
     * --!--
     * CREATE TRIGGER snippets_sync_update
     * AFTER UPDATE ON snippets
     * BEGIN
     *     INSERT INTO snippet_sync_bucket (operation, table_name, row_id, name, snippet_text)
     *     VALUES ('PATCH', 'snippets', NEW.id, NEW.name, NEW.snippet_text);
     * END;
     * --!--
     * CREATE TRIGGER log_snippets_delete
     * AFTER DELETE ON snippets
     * BEGIN
     *     INSERT INTO snippet_sync_bucket (operation, table_name, row_id)
     *     VALUES ('DELETE', 'snippets', OLD.id);
     * END;
     * --!--
     * */

}