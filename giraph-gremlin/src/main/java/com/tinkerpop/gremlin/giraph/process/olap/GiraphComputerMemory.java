package com.tinkerpop.gremlin.giraph.process.olap;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphComputerMemory extends MasterCompute implements Graph.Memory.Computer {

    // TODO: vertex program needs to have ComputeKeys but for master as well.

    private final Logger logger = Logger.getLogger(GiraphComputerMemory.class);
    private VertexProgram vertexProgram;
    private GiraphVertex giraphVertex;
    private int counter = 0;
    private long runtime = java.lang.System.currentTimeMillis();

    public Map<String, Object> memory = new HashMap<>();

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
            this.registerPersistentAggregator("trackPaths", MemoryAggregator.class);
            this.registerAggregator("voteToHalt", MemoryAggregator.class);
            this.registerPersistentAggregator("traversal", MemoryAggregator.class);

            this.vertexProgram.setup(this);
        } catch (Exception e) {
            // java.lang.System.out.println(e + "***" + e.getMessage());
        }
    }

    public void compute() {
        //java.lang.System.out.println("SUPERSTEP: " + this.getSuperstep() + "----" + this.get("voteToHalt"));
        if (this.getSuperstep() > 0) {
            if (this.vertexProgram.terminate(this)) {
                //java.lang.System.out.println("here done:  " + this + " steps: " + this.getSuperstep());
                this.haltComputation();
            }
        }
    }

    public void write(final DataOutput output) {
    }

    public void readFields(final DataInput input) {
    }

    public int getIteration() {
        return null == this.giraphVertex ? (int) this.getSuperstep() : (int) this.giraphVertex.getSuperstep();
    }

    public long getRuntime() {
        return java.lang.System.currentTimeMillis() - this.runtime;
    }

    public Set<String> getVariables() {
        return Collections.emptySet();
    }

    public <R> R get(final String variable) {
        RuleWritable rule = null == this.giraphVertex ? this.getAggregatedValue(variable) : this.giraphVertex.getAggregatedValue(variable);
        //java.lang.System.out.println("Getting variable: " + variable + ":" + rule.getObject() + ":" + rule.getRule());
        return (R) rule.getObject();

        /*if (variable.equals(TraversalVertexProgram.TRACK_PATHS))
            return (R) new Boolean(false);
        else if (variable.equals("voteToHalt")) {
            return (R) new Boolean(this.counter++ > 5);
        } else {
            SSupplier supplier = () -> TinkerGraph.open().V().out().<String>value("name").map(s -> s.get().length());
            return (R) supplier;
        }*/
    }

    public void set(final String variable, Object value) {
        if (null == this.giraphVertex) {
            //java.lang.System.out.println("Setting value: " + variable + ":" + value);
            this.setAggregatedValue(variable, new RuleWritable(RuleWritable.Rule.SET, value));
        } else
            this.giraphVertex.aggregate(variable, new RuleWritable(RuleWritable.Rule.SET, value));
    }

    public void setIfAbsent(final String variable, final Object value) {
        this.set(variable, value);
    }

    public long incr(final String variable, final long delta) {
        return 1;
    }

    public long decr(final String variable, final long delta) {
        return 1;
    }

    public boolean and(final String variable, final boolean bool) {
        //java.lang.System.out.println("ANDing: " + variable + ":" + bool);
        //this.set(variable, this.<Boolean>get(variable) && bool);
        //return this.get(variable);
        if (null == this.giraphVertex) {
            this.setAggregatedValue(variable, new RuleWritable(RuleWritable.Rule.AND, ((RuleWritable) this.getAggregatedValue(variable)).<Boolean>getObject() && bool));
            return ((RuleWritable) this.getAggregatedValue(variable)).getObject();
        } else {
            this.giraphVertex.aggregate(variable, new RuleWritable(RuleWritable.Rule.AND, bool));
            return ((RuleWritable) this.giraphVertex.getAggregatedValue(variable)).getObject();
        }
    }

    public boolean or(final String variable, final boolean bool) {
        //java.lang.System.out.println("ORing: " + variable + ":" + bool);
        // this.set(variable, this.<Boolean>get(variable) || bool);
        // return this.get(variable);
        if (null == this.giraphVertex) {
            this.setAggregatedValue(variable, new RuleWritable(RuleWritable.Rule.OR, ((RuleWritable) this.getAggregatedValue(variable)).<Boolean>getObject() || bool));
            return ((RuleWritable) this.getAggregatedValue(variable)).getObject();
        } else {
            this.giraphVertex.aggregate(variable, new RuleWritable(RuleWritable.Rule.OR, bool));
            return ((RuleWritable) this.giraphVertex.getAggregatedValue(variable)).getObject();
        }
    }

    public Graph getGraph() {
        return EmptyGraph.instance();
    }

}
