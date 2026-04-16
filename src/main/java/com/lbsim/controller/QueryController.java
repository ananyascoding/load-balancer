package com.lbsim.controller;

import com.lbsim.loadbalancer.LoadBalancerService;
import com.lbsim.model.QueryResult;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Controller
public class QueryController {

    private final LoadBalancerService lb;
    public QueryController(LoadBalancerService lb) { this.lb = lb; }

    @GetMapping("/")
    public String dashboard() { return "dashboard"; }

    /**
     * SELECT — routed to one node by algorithm.
     * Params: sql, algorithm (ROUND_ROBIN|LEAST_CONNECTIONS|WEIGHTED_ROUND_ROBIN), clientId
     */
    @PostMapping("/api/query")
    @ResponseBody
    public ResponseEntity<QueryResult> query(
            @RequestParam String sql,
            @RequestParam(defaultValue = "ROUND_ROBIN") String algorithm,
            @RequestParam(defaultValue = "Client-1")    String clientId) {
        return ResponseEntity.ok(lb.executeSelect(sql, algorithm, clientId));
    }

    /**
     * INSERT/UPDATE/DELETE — algorithm picks primary node, then replicates to other 2.
     * Params: sql, algorithm, clientId
     * The algorithm param is now meaningful here too — it decides WHICH node
     * receives the write first (primary target).
     */
    @PostMapping("/api/write")
    @ResponseBody
    public ResponseEntity<QueryResult> write(
            @RequestParam String sql,
            @RequestParam(defaultValue = "ROUND_ROBIN") String algorithm,
            @RequestParam(defaultValue = "Client-1")    String clientId) {
        return ResponseEntity.ok(lb.executeWrite(sql, algorithm, clientId));
    }

    @GetMapping("/api/stats")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> stats() {
        return ResponseEntity.ok(lb.getStats());
    }
}
