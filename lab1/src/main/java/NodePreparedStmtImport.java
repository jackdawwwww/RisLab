import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;
import java.util.function.Supplier;
import com.fasterxml.jackson.databind.ObjectMapper;
import generated.Node;
import generated.Tag;

public class NodePreparedStmtImport implements DataLoader {

    private boolean batchMode;
    private int maxBatchCapacity = 1000;

    public NodePreparedStmtImport(boolean batchMode) {
        this.batchMode = batchMode;
    }

    @Override
    public String[] getTables() {
        return new String[]{"tag", "node", "node_json", "osm_user"};
    }

    @Override
    public int loadData(Iterable<Node> nodes, Connection connection, Supplier<Boolean> cond) {
        int count = 0;
        int osmCount = 0;
        int nodeCount = 0;
        try {
            PreparedStatement insertUser = connection.prepareStatement("insert into osm_user (uid, username) values(?, ?)");
            PreparedStatement insertNode = connection.prepareStatement("insert into node (id, version, cdate, uid, changeset, lat, lon) " +
                    "values(?, ?, ?, ?, ?, ?, ?)");
            Set<BigInteger> users = new HashSet<>();

            for (Node node : nodes) {
                if (cond.get()) break;
                if (!users.contains(node.getUid())) {
                    insertUser.setLong(1, node.getUid().longValue());
                    insertUser.setString(2, node.getUser());

                    if (batchMode) {
                        if (osmCount <= maxBatchCapacity) {
                            osmCount++;
                            insertUser.addBatch();
                        } else {
                            osmCount = 0;
                            insertUser.executeBatch();
                        }
                    } else {
                        insertUser.executeUpdate();
                    }

                    users.add(node.getUid());
                }
                insertNode.setLong(1, node.getId().longValue());
                insertNode.setLong(2, node.getVersion().longValue());
                Timestamp timestamp = new Timestamp(node.getTimestamp().toGregorianCalendar().getTimeInMillis());
                insertNode.setTimestamp(3, timestamp);
                insertNode.setLong(4, node.getUid().longValue());
                insertNode.setLong(5, node.getChangeset().longValue());
                if (node.getLat() != null)
                    insertNode.setDouble(6, node.getLat());
                else
                    insertNode.setNull(6, Types.DOUBLE);

                if (node.getLon() != null)
                    insertNode.setDouble(7, node.getLon());
                else
                    insertNode.setNull(7, Types.DOUBLE);

                if (batchMode) {
                    if (nodeCount <= maxBatchCapacity) {
                        nodeCount++;
                        insertNode.addBatch();
                    } else {
                        nodeCount = 0;
                        osmCount = 0;
                        insertUser.executeBatch();  //для сохранения целостности
                        insertNode.executeBatch();
                    }
                } else {
                    insertNode.executeUpdate();
                }

                count++;
            }
            if (osmCount != 0 ) {
                insertUser.executeBatch();
            }
            if (nodeCount != 0) {
                insertNode.executeBatch();
            }
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return count;
    }
}

