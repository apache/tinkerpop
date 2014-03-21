package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.giraph.process.olap.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.olap.util.MemoryAggregator;
import com.tinkerpop.gremlin.giraph.process.olap.util.RuleWritable;
import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.EmptyGraph;
import org.apache.giraph.master.MasterCompute;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphComputerMemory extends MasterCompute implements Graph.Memory.Computer {

    // TODO: vertex program needs to have ComputeKeys but for master as well.

    private final Logger LOGGER = Logger.getLogger(GiraphComputerMemory.class);
    private VertexProgram vertexProgram;
    private GiraphVertex giraphVertex;
    private long runtime = System.currentTimeMillis();

    public GiraphComputerMemory() {
        this.giraphVertex = null;
        this.vertexProgram = null;
        this.initialize();
    }

    public GiraphComputerMemory(final GiraphVertex giraphVertex) {
        this.giraphVertex = giraphVertex;
        this.initialize();

    }

    public void initialize() {
        try {
            this.vertexProgram = (VertexProgram) new ObjectInputStream(new FileInputStream(GiraphGraphComputer.VERTEX_PROGRAM)).readObject();
            this.registerAggregator("voteToHalt", MemoryAggregator.class);
            this.vertexProgram.setup(ConfUtil.apacheConfiguration(this.getConf()), this);
        } catch (Exception e) {
            // System.out.println(e + "***" + e.getMessage());
        }
    }

    public void compute() {
        //System.out.println("SUPERSTEP: " + this.getSuperstep() + "----" + this.get("voteToHalt"));
        if (!this.isInitialIteration()) {
            if (this.vertexProgram.terminate(this)) {
                this.haltComputation();
            }
        }
    }

    public int getIteration() {
        return null == this.giraphVertex ? (int) this.getSuperstep() : (int) this.giraphVertex.getSuperstep();
    }

    public long getRuntime() {
        return System.currentTimeMillis() - this.runtime;
    }

    public Set<String> getVariables() {
        return Collections.emptySet();
    }

    public <R> R get(final String variable) {
        final RuleWritable rule = (null == this.giraphVertex) ? this.getAggregatedValue(variable) : this.giraphVertex.getAggregatedValue(variable);
        return (R) rule.getObject();
    }

    public void set(final String variable, Object value) {
        if (null == this.giraphVertex)
            this.setAggregatedValue(variable, new RuleWritable(RuleWritable.Rule.SET, value));
        else
            this.giraphVertex.aggregate(variable, new RuleWritable(RuleWritable.Rule.SET, value));
    }

    public void setIfAbsent(final String variable, final Object value) {
        if (null == this.giraphVertex)
            this.setAggregatedValue(variable, new RuleWritable(RuleWritable.Rule.SET_IF_ABSENT, value));
        else
            this.giraphVertex.aggregate(variable, new RuleWritable(RuleWritable.Rule.SET_IF_ABSENT, value));
    }

    public boolean and(final String variable, final boolean bool) {
        //System.out.println("ANDing: " + variable + ":" + bool);
        if (null == this.giraphVertex) {
            this.setAggregatedValue(variable, new RuleWritable(RuleWritable.Rule.AND, ((RuleWritable) this.getAggregatedValue(variable)).<Boolean>getObject() && bool));
            return ((RuleWritable) this.getAggregatedValue(variable)).getObject();
        } else {
            this.giraphVertex.aggregate(variable, new RuleWritable(RuleWritable.Rule.AND, bool));
            return ((RuleWritable) this.giraphVertex.getAggregatedValue(variable)).getObject();
        }
    }

    public boolean or(final String variable, final boolean bool) {
        //System.out.println("ORing: " + variable + ":" + bool);
        if (null == this.giraphVertex) {
            this.setAggregatedValue(variable, new RuleWritable(RuleWritable.Rule.OR, ((RuleWritable) this.getAggregatedValue(variable)).<Boolean>getObject() || bool));
            return ((RuleWritable) this.getAggregatedValue(variable)).getObject();
        } else {
            this.giraphVertex.aggregate(variable, new RuleWritable(RuleWritable.Rule.OR, bool));
            // todo: void or?
            return ((RuleWritable) this.giraphVertex.getAggregatedValue(variable)).getObject();
        }
    }


    public long incr(final String variable, final long delta) {
        return 1;
    }

    public Graph getGraph() {
        return EmptyGraph.instance();
    }

    public void write(final DataOutput output) {
    }

    public void readFields(final DataInput input) {
    }
}
