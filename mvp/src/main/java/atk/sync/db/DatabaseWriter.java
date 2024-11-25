package atk.sync.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static atk.sync.model.SyncRule.*;

public class DatabaseWriter {

    private final String jdbcPath;

    public DatabaseWriter(String jdbcPath) {
        this.jdbcPath = jdbcPath;
    }

    public boolean executeWriteTransaction(List<SqlStatement> sqlStatements) {
        try (Connection conn = DriverManager.getConnection(jdbcPath)) {
            conn.setAutoCommit(true);
            execute(conn, sqlStatements);
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    private void execute(Connection conn, List<SqlStatement> sqlStatements) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            for (SqlStatement sqlStatement : sqlStatements) {
                statement.execute(sqlStatement.sqlStatement());
            }
        } catch (SQLException e) {
            conn.rollback();
        }
    }

}
