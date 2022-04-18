import generated.Node;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Supplier;

public interface DataLoader {
    default void cleanup(Connection connection) throws SQLException {
        Statement stmt = connection.createStatement();
        for (String table: getTables()) {
            stmt.execute("delete from " + table);
        }
    }
    String[] getTables();
    int loadData(Iterable<Node> nodes, Connection connection, Supplier<Boolean> cond);
}
