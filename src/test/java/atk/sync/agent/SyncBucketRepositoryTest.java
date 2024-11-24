package atk.sync.agent;

import atk.sync.db.SyncBucketRepository;
import atk.sync.util.OperationConvertor;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SyncBucketRepositoryTest extends BaseTest {

    @Test
    void shouldAccumulatedOperationInSyncBuckets() throws SQLException {
        var sqlStatements = List.of("INSERT INTO snippets (id,name,snippet_text) VALUES (1,'a1', 'text1')",
                "UPDATE snippets SET name='a2' WHERE id=1",
                "DELETE FROM snippets where id=1");
        //when sql statements are executed
        executeSqlStatements(sqlStatements);
        SyncBucketRepository repo =
                new SyncBucketRepository("snippet_sync_bucket", testDbPath);
        //then 3 operations should be in the operations log
        var collectedOperations = repo.getAllOperations();
        assertEquals(3, collectedOperations.size());
        executeSqlStatements(OperationConvertor.toSqlStatement(collectedOperations, tableColumnTypes));
    }
}