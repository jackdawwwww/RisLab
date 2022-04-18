import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import generated.Node;
import generated.Tag;

public class NodeStmtImport implements DataLoader {

    @Override
    public String[] getTables() {
        return new String[]{"node", "tag", "osm_user"};
    }

    @Override
    public int loadData(Iterable<Node> nodes, Connection connection, Supplier<Boolean> cond){
        int count = 0;
        try {
            Statement stmt = connection.createStatement();
            Set<BigInteger> users = new HashSet<>();

            for (Node node : nodes) {
                if (cond.get()) break;
                String userName = node.getUser();
                if (!users.contains(node.getUid())) {
                    stmt.executeUpdate("insert into osm_user values(" + node.getUid() + ", '" + userName.replace("'", "''") + "')");
                    users.add(node.getUid());
                }
                String sql = "insert into node (id, version, cdate, uid, changeset, lat, lon) values({0,number,#}, {1,number,#}, ''{2}'', {3,number,#}, {4,number,#}, {5,number,#}, {6,number,#})";
                String sqlData = MessageFormat.format(sql, node.getId(), node.getVersion(),
                        node.getTimestamp(), node.getUid(), node.getChangeset(), node.getLat(), node.getLon());
                stmt.executeUpdate(sqlData);
                if (node.getTag() != null) {
                    for (Tag t : node.getTag()) {
                        stmt.executeUpdate("insert into tag (nodeid, k, v) values(" + node.getId() + ", '" + t.getK() + "', '" + t.getV() + "')");
                    }
                }
                count++;
            }
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return count;
    }
}
