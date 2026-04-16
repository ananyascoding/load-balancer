package com.lbsim.loadbalancer;

import com.lbsim.model.NodeStage;
import com.lbsim.model.QueryResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class LoadBalancerService {

    private final List<DatabaseNode> nodes = new ArrayList<>();

    private final AtomicInteger rrIndex = new AtomicInteger(0);
    private List<DatabaseNode>  weightedPool;

    private final LinkedList<QueryResult> requestLog = new LinkedList<>();
    private static final int LOG_LIMIT = 100;

    private final String username;
    private final String password;
    private final int    maxRetries;
    private final long   retryDelayMs;

    public LoadBalancerService(
            @Value("${db.node1.url}")    String url1, @Value("${db.node1.port}") int p1, @Value("${db.node1.weight}") int w1,
            @Value("${db.node2.url}")    String url2, @Value("${db.node2.port}") int p2, @Value("${db.node2.weight}") int w2,
            @Value("${db.node3.url}")    String url3, @Value("${db.node3.port}") int p3, @Value("${db.node3.weight}") int w3,
            @Value("${db.username}")     String username,
            @Value("${db.password}")     String password,
            @Value("${db.replication.maxRetries}")   int maxRetries,
            @Value("${db.replication.retryDelayMs}") long retryDelayMs) {

        this.username     = username;
        this.password     = password;
        this.maxRetries   = maxRetries;
        this.retryDelayMs = retryDelayMs;

        nodes.add(new DatabaseNode("MySQL-Node-1", url1, p1, w1));
        nodes.add(new DatabaseNode("MySQL-Node-2", url2, p2, w2));
        nodes.add(new DatabaseNode("MySQL-Node-3", url3, p3, w3));

        buildWeightedPool();
    }

    private void buildWeightedPool() {
        weightedPool = new ArrayList<>();
        for (DatabaseNode n : nodes)
            for (int i = 0; i < n.getWeight(); i++) weightedPool.add(n);
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  SELECT — single node, chosen by algorithm
    // ═══════════════════════════════════════════════════════════════════════
    public QueryResult executeSelect(String sql, String algorithm, String clientId) {
        if (!sql.trim().toUpperCase().startsWith("SELECT"))
            return rejected(sql, algorithm, clientId, "Use /api/write for INSERT/UPDATE/DELETE.");

        DatabaseNode node = selectNode(algorithm);
        node.incrementActive();
        long start = System.currentTimeMillis();

        QueryResult result = base(sql, algorithm, clientId);
        result.setNodeName(node.getName());
        result.setPort(node.getPort());
        result.setOperationType("SELECT");
        result.setDistributed(false);

        List<Map<String, Object>> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(node.getJdbcUrl(), username, password);
             Statement  stmt = conn.createStatement();
             ResultSet  rs   = stmt.executeQuery(sql)) {

            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= cols; i++)
                    row.put(meta.getColumnName(i), rs.getObject(i));
                rows.add(row);
            }
            result.setStatus("SUCCESS");
        } catch (Exception e) {
            result.setStatus("ERROR");
            result.setError(e.getMessage());
        } finally {
            node.decrementActive();
        }

        long ms = System.currentTimeMillis() - start;
        result.setRows(rows);
        result.setResponseTimeMs(ms);
        node.recordRequest("SUCCESS".equals(result.getStatus()), ms);
        log(result);
        return result;
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  WRITE — algorithm picks primary node, then replicates to the other 2
    //
    //  Key design:
    //    1. selectNode(algorithm) picks which node is the primary target.
    //       This is the SAME algorithm used for SELECTs — Round Robin,
    //       Least Connections, or Weighted Round Robin.
    //    2. The pipeline order is:  [primaryNode, otherNode1, otherNode2]
    //       where otherNode1 and otherNode2 are the remaining nodes in
    //       their natural index order.
    //    3. Execute SQL on each in order, holding transactions open (autoCommit=false).
    //    4. If any node fails (after retries) → rollback ALL opened connections.
    //    5. If all 3 held → commit all 3 in order.
    // ═══════════════════════════════════════════════════════════════════════
    public QueryResult executeWrite(String sql, String algorithm, String clientId) {
        String upper = sql.trim().toUpperCase();
        String opType;
        if      (upper.startsWith("INSERT")) opType = "INSERT";
        else if (upper.startsWith("UPDATE")) opType = "UPDATE";
        else if (upper.startsWith("DELETE")) opType = "DELETE";
        else return rejected(sql, algorithm, clientId,
                "Only INSERT, UPDATE, DELETE are allowed on /api/write.");

        // ── Step 1: algorithm picks the primary write target ──────────────
        DatabaseNode primaryNode = selectNode(algorithm);

        // ── Step 2: build ordered pipeline [primary, ...rest in index order]
        List<DatabaseNode> pipeline = new ArrayList<>();
        pipeline.add(primaryNode);
        for (DatabaseNode n : nodes)
            if (n != primaryNode) pipeline.add(n);

        // Build stage trackers
        List<NodeStage> stages = new ArrayList<>();
        for (int i = 0; i < pipeline.size(); i++)
            stages.add(new NodeStage(
                pipeline.get(i).getName(),
                pipeline.get(i).getPort(),
                i == 0,   // primaryTarget = true only for index 0
                i
            ));

        long start = System.currentTimeMillis();
        QueryResult result = base(sql, algorithm, clientId);
        result.setOperationType(opType);
        result.setDistributed(true);
        result.setNodeName(primaryNode.getName() + " → others");
        result.setPrimaryNodeName(primaryNode.getName());
        result.setPipeline(stages);

        Connection[] conns      = new Connection[pipeline.size()];
        int          affected   = 0;
        int          totalRetries = 0;

        try {
            // ── Step 3: execute on each node in pipeline order ─────────────
            for (int i = 0; i < pipeline.size(); i++) {
                DatabaseNode node  = pipeline.get(i);
                NodeStage    stage = stages.get(i);
                stage.setState(NodeStage.State.EXECUTING);

                long stageStart  = System.currentTimeMillis();
                int  retriesUsed = 0;
                boolean nodeOk   = false;
                Exception lastEx = null;

                // Primary gets 1 attempt; replicas get maxRetries+1 attempts
                int maxAttempts = (i == 0) ? 1 : (maxRetries + 1);

                for (int attempt = 0; attempt < maxAttempts; attempt++) {
                    if (attempt > 0) {
                        retriesUsed++;
                        totalRetries++;
                        stage.setDetail("Retry " + retriesUsed + "/" + maxRetries + "…");
                        try { Thread.sleep(retryDelayMs); }
                        catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    }
                    try {
                        // Close any failed previous attempt connection
                        if (conns[i] != null) {
                            try { conns[i].rollback(); conns[i].close(); node.decrementActive(); }
                            catch (Exception ignored) {}
                            conns[i] = null;
                        }
                        conns[i] = DriverManager.getConnection(node.getJdbcUrl(), username, password);
                        conns[i].setAutoCommit(false);
                        node.incrementActive();

                        try (Statement stmt = conns[i].createStatement()) {
                            int rows = stmt.executeUpdate(sql);
                            if (i == 0) affected = rows;
                        }
                        nodeOk = true;
                        lastEx = null;
                        break;
                    } catch (Exception e) {
                        lastEx = e;
                        if (conns[i] != null) {
                            try { conns[i].close(); node.decrementActive(); } catch (Exception ignored) {}
                            conns[i] = null;
                        }
                    }
                }

                stage.setRetriesUsed(retriesUsed);
                stage.setStageMs(System.currentTimeMillis() - stageStart);
                node.addRetries(retriesUsed);

                if (!nodeOk) {
                    // This node failed — mark it, mark remaining as pending, rollback all
                    stage.setState(NodeStage.State.FAILED);
                    stage.setDetail(lastEx != null ? lastEx.getMessage() : "Unknown error");
                    for (int j = i + 1; j < pipeline.size(); j++)
                        stages.get(j).setState(NodeStage.State.PENDING);

                    rollbackAll(conns, stages, pipeline, i);

                    result.setStatus("ROLLED_BACK");
                    result.setError("[" + node.getName() + "] failed after " + retriesUsed
                            + " retr" + (retriesUsed == 1 ? "y" : "ies") + ": "
                            + (lastEx != null ? lastEx.getMessage() : "unknown"));
                    result.setTotalRetries(totalRetries);
                    result.setAffectedRows(0);
                    long ms = System.currentTimeMillis() - start;
                    result.setResponseTimeMs(ms);
                    for (DatabaseNode n : nodes) n.recordRequest(false, ms);
                    log(result);
                    return result;
                }

                stage.setState(NodeStage.State.HELD);
                stage.setDetail(i == 0
                    ? "Primary target — executed & held (algorithm: " + algorithm + ")"
                    : "Replica — executed & held, awaiting commit signal");
            }

            // ── Step 4: all held → commit in pipeline order ────────────────
            for (int i = 0; i < pipeline.size(); i++) {
                NodeStage    stage = stages.get(i);
                DatabaseNode node  = pipeline.get(i);
                try {
                    conns[i].commit();
                    stage.setState(NodeStage.State.COMMITTED);
                    stage.setDetail("Committed ✓");
                    node.recordWrite();
                } catch (Exception e) {
                    stage.setState(NodeStage.State.FAILED);
                    stage.setDetail("Commit failed: " + e.getMessage());
                    // Rollback everything not yet committed
                    for (int j = i + 1; j < pipeline.size(); j++) {
                        try { conns[j].rollback(); } catch (Exception ignored) {}
                        stages.get(j).setState(NodeStage.State.ROLLED_BACK);
                        stages.get(j).setDetail("Rolled back — commit failed on " + node.getName());
                        pipeline.get(j).recordRollback();
                    }
                    result.setStatus("ROLLED_BACK");
                    result.setError("Commit failed on " + node.getName() + ": " + e.getMessage());
                    result.setTotalRetries(totalRetries);
                    result.setAffectedRows(0);
                    long ms = System.currentTimeMillis() - start;
                    result.setResponseTimeMs(ms);
                    for (DatabaseNode n : nodes) n.recordRequest(false, ms);
                    log(result);
                    return result;
                }
            }

            result.setStatus("COMMITTED");
            result.setAffectedRows(affected);
            result.setTotalRetries(totalRetries);

        } catch (Exception e) {
            rollbackAll(conns, stages, pipeline, pipeline.size() - 1);
            result.setStatus("ROLLED_BACK");
            result.setError("Unexpected error: " + e.getMessage());
        } finally {
            for (int i = 0; i < conns.length; i++) {
                if (conns[i] != null) {
                    try { conns[i].close(); } catch (Exception ignored) {}
                    pipeline.get(i).decrementActive();
                }
            }
        }

        long ms = System.currentTimeMillis() - start;
        result.setResponseTimeMs(ms);
        boolean ok = "COMMITTED".equals(result.getStatus());
        for (DatabaseNode n : nodes) n.recordRequest(ok, ms);
        log(result);
        return result;
    }

    // ── Rollback all open connections up to index upTo (inclusive) ────────
    private void rollbackAll(Connection[] conns, List<NodeStage> stages,
                             List<DatabaseNode> pipeline, int upTo) {
        for (int i = 0; i <= upTo && i < conns.length; i++) {
            if (conns[i] != null) {
                NodeStage stage = stages.get(i);
                if (stage.getState() != NodeStage.State.FAILED) {
                    try {
                        conns[i].rollback();
                        stage.setState(NodeStage.State.ROLLED_BACK);
                        stage.setDetail("Rolled back — transaction aborted");
                        pipeline.get(i).recordRollback();
                    } catch (Exception ex) {
                        stage.setState(NodeStage.State.ROLLED_BACK);
                        stage.setDetail("Rollback error: " + ex.getMessage());
                    }
                }
                try { conns[i].close(); } catch (Exception ignored) {}
                pipeline.get(i).decrementActive();
                conns[i] = null;
            }
        }
    }

    // ── Node selection (shared by SELECT and WRITE) ───────────────────────
    private DatabaseNode selectNode(String algorithm) {
        return switch (algorithm.toUpperCase()) {
            case "LEAST_CONNECTIONS" -> nodes.stream()
                    .min(Comparator.comparingInt(DatabaseNode::getActiveConnections))
                    .orElse(nodes.get(0));
            case "WEIGHTED_ROUND_ROBIN" ->
                    weightedPool.get(Math.abs(rrIndex.getAndIncrement() % weightedPool.size()));
            default -> // ROUND_ROBIN
                    nodes.get(Math.abs(rrIndex.getAndIncrement() % nodes.size()));
        };
    }

    // ── Helpers ───────────────────────────────────────────────────────────
    private QueryResult base(String sql, String algorithm, String clientId) {
        QueryResult r = new QueryResult();
        r.setSql(sql); r.setAlgorithm(algorithm); r.setClientId(clientId);
        r.setTimestamp(LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        return r;
    }

    private QueryResult rejected(String sql, String algo, String client, String reason) {
        QueryResult r = base(sql, algo, client);
        r.setStatus("REJECTED"); r.setError(reason);
        r.setNodeName("—"); r.setOperationType("UNKNOWN");
        log(r); return r;
    }

    private void log(QueryResult r) {
        synchronized (requestLog) {
            requestLog.addFirst(r);
            if (requestLog.size() > LOG_LIMIT) requestLog.removeLast();
        }
    }

    // ── Stats ─────────────────────────────────────────────────────────────
    public Map<String, Object> getStats() {
        List<Map<String, Object>> nodeStats = new ArrayList<>();
        int grandTotal = nodes.stream().mapToInt(DatabaseNode::getTotalRequests).sum();

        for (DatabaseNode n : nodes) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name",              n.getName());
            m.put("port",              n.getPort());
            m.put("weight",            n.getWeight());
            m.put("activeConnections", n.getActiveConnections());
            m.put("totalRequests",     n.getTotalRequests());
            m.put("successCount",      n.getSuccessCount());
            m.put("errorCount",        n.getErrorCount());
            m.put("writeCount",        n.getWriteCount());
            m.put("rollbackCount",     n.getRollbackCount());
            m.put("retryCount",        n.getRetryCount());
            m.put("avgResponseMs",     String.format("%.1f", n.avgResponseMs()));
            m.put("loadPct",           grandTotal == 0 ? 0 :
                    Math.round((double) n.getTotalRequests() / grandTotal * 100));
            nodeStats.add(m);
        }

        List<QueryResult> snap;
        synchronized (requestLog) { snap = new ArrayList<>(requestLog); }

        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("nodes",         nodeStats);
        stats.put("log",           snap);
        stats.put("totalRequests", grandTotal);
        return stats;
    }

    public List<DatabaseNode> getNodes() { return nodes; }
}
