package atk.sync.agent;

import atk.sync.model.Models;
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

public class BaseTest {

    protected Models.JDBCPath jdbcPath;

    @TempDir
    private Path path;

    @BeforeEach
    public void setUp() throws SQLException, IOException {
        this.jdbcPath = new Models.JDBCPath(path.resolve("test_db"));
        applyInitDb(jdbcPath, "init_db.sql");
    }

    private void applyInitDb(Models.JDBCPath jdbcPath, String initScriptName) throws SQLException, IOException {
        var initScriptPath = Path.of(getClass().getClassLoader().getResource(initScriptName).getPath());
        try (Connection conn = DriverManager.getConnection(jdbcPath.value);
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

    protected void executeSqlStatements(List<Models.SqlStatement> sqlCommands) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcPath.value);
             Statement stmt = conn.createStatement()) {
            for (Models.SqlStatement sqlCommand : sqlCommands) {
                stmt.execute(sqlCommand.value);
            }
        }
    }

}
