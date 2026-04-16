package com.lbsim.model;

/**
 * Tracks one database node's status through the write pipeline.
 * primaryTarget=true means this node was chosen by the load balancing
 * algorithm as the first write target for this transaction.
 */
public class NodeStage {

    public enum State {
        PENDING,       // not yet attempted
        EXECUTING,     // SQL being sent
        HELD,          // executed OK — transaction open, waiting for commit signal
        COMMITTED,     // committed successfully
        FAILED,        // failed after all retries
        ROLLED_BACK    // rolled back because another node failed
    }

    private final String  nodeName;
    private final int     port;
    private final boolean primaryTarget;  // chosen by algorithm as first write node
    private final int     pipelineOrder;  // 0 = first executed, 1 = second, 2 = third

    private State  state        = State.PENDING;
    private int    retriesUsed  = 0;
    private String detail       = "";
    private long   stageMs      = 0;

    public NodeStage(String nodeName, int port, boolean primaryTarget, int pipelineOrder) {
        this.nodeName      = nodeName;
        this.port          = port;
        this.primaryTarget = primaryTarget;
        this.pipelineOrder = pipelineOrder;
    }

    public String  getNodeName()       { return nodeName; }
    public int     getPort()           { return port; }
    public boolean isPrimaryTarget()   { return primaryTarget; }
    public int     getPipelineOrder()  { return pipelineOrder; }
    public State   getState()          { return state; }
    public int     getRetriesUsed()    { return retriesUsed; }
    public String  getDetail()         { return detail; }
    public long    getStageMs()        { return stageMs; }

    public void setState(State s)      { this.state = s; }
    public void setRetriesUsed(int r)  { this.retriesUsed = r; }
    public void setDetail(String d)    { this.detail = d; }
    public void setStageMs(long ms)    { this.stageMs = ms; }
}
