import java.util.Map;

public class Statistic {
    Map<String, Integer> usersChanges;
    Map<String, Integer> nodesIds;

    public Statistic(Map<String, Integer> usersChanges, Map<String, Integer> nodesIds) {
        this.usersChanges = usersChanges;
        this.nodesIds = nodesIds;
    }
}
