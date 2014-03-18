package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.EmptyGraph;
import com.tinkerpop.gremlin.util.function.SSupplier;
import com.tinkerpop.tinkergraph.structure.TinkerGraph;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
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

    private final Logger logger = Logger.getLogger(GiraphComputerMemory.class);
    private VertexProgram vertexProgram;
    private GiraphVertex giraphVertex;
    private int counter = 0;
    private long runtime = java.lang.System.currentTimeMillis();

    public GiraphComputerMemory() {
        this.giraphVertex = null;
        this.vertexProgram = null;
        this.initialize();
    }

    public GiraphComputerMemory(final GiraphVertex giraphVertex) {
        this.giraphVertex = giraphVertex;
        this.initialize();

    }

    public <T> void registerComputerVariable(final String variable, final T initialValue) {
        /*this.registerAggregator(variable, new BasicAggregator<T>() {
            public T createInitialValue() {
                return initialValue;
            }

            public void aggregate(T t) {

            }
        });*/
    }

    public void initialize() {
        try {
            this.vertexProgram = (VertexProgram) new ObjectInputStream(new FileInputStream(GiraphGraphComputer.VERTEX_PROGRAM)).readObject();
            //this.registerAggregator("or", BooleanOrAggregator.class);
        } catch (Exception e) {
            java.lang.System.out.println(e.getMessage());
        }
    }

    public void compute() {
        if (this.vertexProgram.terminate(this)) {
            java.lang.System.out.println("here done:  " + this + " steps: " + this.getSuperstep());
            this.haltComputation();
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
        //return (R) this.getAggregatedValue(variable);
        if (variable.equals(TraversalVertexProgram.TRACK_PATHS))
            return (R) new Boolean(false);
        else if (variable.equals("voteToHalt")) {
            return (R) new Boolean(this.counter++ > 5);
        } else {
            SSupplier supplier = () -> TinkerGraph.open().V().out().<String>value("name").map(s -> s.get().length());
            return (R) supplier;
        }
    }

    public void set(final String variable, Object value) {
        this.giraphVertex.aggregate(variable, new ObjectWritable(value));
    }

    public void setIfAbsent(final String variable, final Object value) {
        this.giraphVertex.aggregate(variable, new ObjectWritable(value));
    }

    public long incr(final String variable, final long delta) {
        this.giraphVertex.aggregate(variable, new LongWritable(delta));
        return this.giraphVertex.<LongWritable>getAggregatedValue(variable).get();
    }

    public long decr(final String variable, final long delta) {
        this.giraphVertex.aggregate(variable, new LongWritable(delta));
        return this.giraphVertex.<LongWritable>getAggregatedValue(variable).get();
    }

    public boolean and(final String variable, final boolean bool) {
        this.giraphVertex.aggregate(variable, new BooleanWritable(bool));
        return this.giraphVertex.<BooleanWritable>getAggregatedValue(variable).get();
    }

    public boolean or(final String variable, final boolean bool) {
        this.giraphVertex.aggregate(variable, new BooleanWritable(bool));
        return this.giraphVertex.<BooleanWritable>getAggregatedValue(variable).get();
    }

    public Graph getGraph() {
        return EmptyGraph.instance();
    }

}
