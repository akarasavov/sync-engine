package atk.sync.agent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.StringJoiner;

import static atk.sync.model.SyncRule.SqlStatement;

public class BaseTest {

    protected Path testDbPath;

    @TempDir
    private Path path;
    protected String dbUrl;

    @BeforeEach
    public void setUp() throws SQLException, IOException {
        this.testDbPath = path.resolve("test_db");
        this.dbUrl = "jdbc:sqlite:" + testDbPath;
        applyInitDb(dbUrl, "init_db.sql");
    }

    private void applyInitDb(String dbUrl, String initScriptName) throws SQLException, IOException {
        var initScriptPath = Path.of(getClass().getClassLoader().getResource(initScriptName).getPath());
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            var stringJoiner = new StringJoiner("\n");
            for (String line : Files.readAllLines(initScriptPath)) {
                if (line.equals("--!--")) {
                    stmt.execute(stringJoiner.toString());
                    stringJoiner = new StringJoiner("\n");
                }
                stringJoiner.add(line);
            }
        }
    }

    protected void executeSqlStatements(List<SqlStatement> sqlCommands) throws SQLException {
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            for (SqlStatement sqlCommand : sqlCommands) {
                stmt.execute(sqlCommand.sqlStatement());
            }
        }
    }

}
