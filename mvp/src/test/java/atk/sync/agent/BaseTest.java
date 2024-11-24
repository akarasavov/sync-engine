package atk.sync.agent;

import atk.sync.util.OperationConvertor;
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
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class BaseTest {

    protected Map<String, Map<String, Integer>> tableColumnTypes;
    protected Path testDbPath;

    @TempDir
    private Path path;
    private String dbUrl;

    @BeforeEach
    public void setUp() throws SQLException, IOException {
        this.testDbPath = path.resolve("test_db");
        this.dbUrl = "jdbc:sqlite:" + testDbPath;
        applyInitDb(dbUrl, "init_db.sql");
        this.tableColumnTypes = OperationConvertor.getTableColumnTypes(testDbPath, List.of("snippets"));
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

    protected void executeSqlStatements(List<String> sqlCommands) throws SQLException {
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            for (String sqlCommand : sqlCommands) {
                stmt.execute(sqlCommand);
            }
        }
    }

}
