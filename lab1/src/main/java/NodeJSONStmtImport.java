import generated.Node;
import generated.Tag;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Supplier;

public class NodeJSONStmtImport implements DataLoader {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String[] getTables() {
        return new String[] {"tag", "node", "node_json", "osm_user"};
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
                String sqlData;

                if (node.getTag() != null && !node.getTag().isEmpty()) {
                    String sql = "insert into node_json (id, version, cdate, uid, changeset, lat, lon, tags) values({0,number,#}, {1,number,#}, ''{2}'', {3,number,#}, {4,number,#}, {5,number,#}, {6,number,#}, ''{7}''::json)";
                    sqlData = MessageFormat.format(sql, node.getId(), node.getVersion(),
                            node.getTimestamp(), node.getUid(), node.getChangeset(), node.getLat(), node.getLon(), tagsToJSON(node.getTag()));
                } else {
                    String sql = "insert into node_json (id, version, cdate, uid, changeset, lat, lon) values({0,number,#}, {1,number,#}, ''{2}'', {3,number,#}, {4,number,#}, {5,number,#}, {6,number,#})";
                    sqlData = MessageFormat.format(sql, node.getId(), node.getVersion(),
                            node.getTimestamp(), node.getUid(), node.getChangeset(), node.getLat(), node.getLon());
                }

                stmt.executeUpdate(sqlData);
                count++;
            }
        } catch (SQLException | IOException exception) {
            exception.printStackTrace();
        }
        return count;
    }
    private String tagsToJSON(List<Tag> tags) throws IOException {
        StringWriter stringWriter = new StringWriter();
        Map<String, String> tagsMap = new HashMap<>();
        for (Tag tag : tags) {
            tagsMap.put(tag.getK(), tag.getV());
        }
        objectMapper.writeValue(stringWriter, tagsMap);
        return stringWriter.toString();
    }
}
