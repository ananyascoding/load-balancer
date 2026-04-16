package com.lbsim.model;

import java.util.List;
import java.util.Map;

public class QueryResult {

    // Common
    private String  nodeName;
    private int     port;
    private String  algorithm;
    private String  sql;
    private String  clientId;
    private String  status;
    private long    responseTimeMs;
    private String  timestamp;
    private String  error;
    private String  operationType;

    // SELECT
    private List<Map<String, Object>> rows;

    // WRITE
    private boolean         distributed;
    private int             affectedRows;
    private List<NodeStage> pipeline;
    private int             totalRetries;
    private String          primaryNodeName;  // which node the algorithm chose as primary

    public String  getNodeName()               { return nodeName; }
    public void    setNodeName(String v)       { nodeName = v; }
    public int     getPort()                   { return port; }
    public void    setPort(int v)              { port = v; }
    public String  getAlgorithm()              { return algorithm; }
    public void    setAlgorithm(String v)      { algorithm = v; }
    public String  getSql()                    { return sql; }
    public void    setSql(String v)            { sql = v; }
    public String  getClientId()               { return clientId; }
    public void    setClientId(String v)       { clientId = v; }
    public String  getStatus()                 { return status; }
    public void    setStatus(String v)         { status = v; }
    public long    getResponseTimeMs()         { return responseTimeMs; }
    public void    setResponseTimeMs(long v)   { responseTimeMs = v; }
    public String  getTimestamp()              { return timestamp; }
    public void    setTimestamp(String v)      { timestamp = v; }
    public String  getError()                  { return error; }
    public void    setError(String v)          { error = v; }
    public String  getOperationType()          { return operationType; }
    public void    setOperationType(String v)  { operationType = v; }

    public List<Map<String, Object>> getRows()        { return rows; }
    public void setRows(List<Map<String, Object>> v)  { rows = v; }

    public boolean         isDistributed()              { return distributed; }
    public void            setDistributed(boolean v)    { distributed = v; }
    public int             getAffectedRows()             { return affectedRows; }
    public void            setAffectedRows(int v)        { affectedRows = v; }
    public List<NodeStage> getPipeline()                 { return pipeline; }
    public void            setPipeline(List<NodeStage> v){ pipeline = v; }
    public int             getTotalRetries()             { return totalRetries; }
    public void            setTotalRetries(int v)        { totalRetries = v; }
    public String          getPrimaryNodeName()          { return primaryNodeName; }
    public void            setPrimaryNodeName(String v)  { primaryNodeName = v; }
}
