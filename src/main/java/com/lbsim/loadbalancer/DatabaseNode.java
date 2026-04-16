package com.lbsim.loadbalancer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DatabaseNode {

    private final String  name;
    private final String  jdbcUrl;
    private final int     port;
    private final int     weight;

    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicInteger totalRequests     = new AtomicInteger(0);
    private final AtomicInteger successCount      = new AtomicInteger(0);
    private final AtomicInteger errorCount        = new AtomicInteger(0);
    private final AtomicInteger writeCount        = new AtomicInteger(0);
    private final AtomicInteger rollbackCount     = new AtomicInteger(0);
    private final AtomicInteger retryCount        = new AtomicInteger(0);
    private final AtomicLong    totalResponseMs   = new AtomicLong(0);

    public DatabaseNode(String name, String jdbcUrl, int port, int weight) {
        this.name    = name;
        this.jdbcUrl = jdbcUrl;
        this.port    = port;
        this.weight  = weight;
    }

    public void recordRequest(boolean success, long ms) {
        totalRequests.incrementAndGet();
        totalResponseMs.addAndGet(ms);
        if (success) successCount.incrementAndGet();
        else         errorCount.incrementAndGet();
    }
    public void recordWrite()       { writeCount.incrementAndGet(); }
    public void recordRollback()    { rollbackCount.incrementAndGet(); }
    public void addRetries(int n)   { retryCount.addAndGet(n); }
    public void incrementActive()   { activeConnections.incrementAndGet(); }
    public void decrementActive()   { if (activeConnections.get() > 0) activeConnections.decrementAndGet(); }

    public double avgResponseMs() {
        int t = totalRequests.get();
        return t == 0 ? 0.0 : (double) totalResponseMs.get() / t;
    }

    public String  getName()              { return name; }
    public String  getJdbcUrl()           { return jdbcUrl; }
    public int     getPort()              { return port; }
    public int     getWeight()            { return weight; }
    public int     getActiveConnections() { return activeConnections.get(); }
    public int     getTotalRequests()     { return totalRequests.get(); }
    public int     getSuccessCount()      { return successCount.get(); }
    public int     getErrorCount()        { return errorCount.get(); }
    public int     getWriteCount()        { return writeCount.get(); }
    public int     getRollbackCount()     { return rollbackCount.get(); }
    public int     getRetryCount()        { return retryCount.get(); }
    public long    getTotalResponseMs()   { return totalResponseMs.get(); }
}
