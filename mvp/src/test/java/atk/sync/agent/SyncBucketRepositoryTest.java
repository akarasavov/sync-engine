package atk.sync.agent;

import atk.sync.db.SyncBucketRepository;
import atk.sync.model.SyncRule;
import atk.sync.util.MetaTableReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import static atk.sync.model.Models.ConflictKey;
import static atk.sync.model.Models.ConflictResolutionStrategy;
import static atk.sync.model.Models.SqlStatement;
import static atk.sync.model.Models.SyncBucketName;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SyncBucketRepositoryTest extends BaseTest {

    private final SyncBucketName syncBucketName = new SyncBucketName("snippet_sync_bucket");
    private final SyncRule syncRule = new SyncRule(syncBucketName, ConflictResolutionStrategy.LWW,
            ConflictKey.idConflictKey(), new SqlStatement("SELECT * FROM snippets"));

    @BeforeEach
    @Override
    public void setUp() throws SQLException, java.io.IOException {
        super.setUp();
        var tableMetadata = MetaTableReader.getTableMetadata(jdbcPath, syncRule.selectStatement);
        var sqlStatements = List.of(syncRule.generateCreateTableStatement(tableMetadata),
                syncRule.generateInsertTriggerStatement(tableMetadata),
                syncRule.generateUpdateTriggerStatement(tableMetadata),
                syncRule.generateDeleteTriggerStatement(tableMetadata));
        executeSqlStatements(sqlStatements);
    }

    @Test
    void shouldAccumulatedOperationInSyncBuckets() throws SQLException {
        var sqlStatements = Stream.of("INSERT INTO snippets (id,name,snippet_text) VALUES (1,'a1', 'text1')",
                "UPDATE snippets SET name='a2', snippet_text='b2' WHERE id=1",
                "DELETE FROM snippets where id=1").map(SqlStatement::new).toList();
        //when sql statements are executed
        executeSqlStatements(sqlStatements);
        SyncBucketRepository repo =
                new SyncBucketRepository(syncBucketName, jdbcPath);
        //then 3 operations should be in the operations log
        var collectedOperations = repo.getAllOperations();
        assertEquals(3, collectedOperations.size());
    }

    @Test
    void disabledTriggersShouldntProduceOperations() throws SQLException {
        //when triggers are disabled
        executeSqlStatements(syncRule.dropCreatedTriggers());
        var mutationOperations = Stream.of("INSERT INTO snippets (id,name,snippet_text) VALUES (1,'a1', 'text1')",
                "UPDATE snippets SET name='a2', snippet_text='b2' WHERE id=1").map(SqlStatement::new).toList();
        //when mutation are executed
        executeSqlStatements(mutationOperations);

        //then
        SyncBucketRepository repo =
                new SyncBucketRepository(syncBucketName, jdbcPath);

        assertEquals(true, repo.getAllOperations().isEmpty());
    }
}