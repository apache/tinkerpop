package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.EmptyGraph;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphComputerMemory implements Graph.Memory.Computer {

    private final GiraphVertex giraphVertex;

    public GiraphComputerMemory(final GiraphVertex giraphVertex) {
        this.giraphVertex = giraphVertex;
    }

    public int getIteration() {
        return (int) this.giraphVertex.getSuperstep();
    }

    public long getRuntime() {
        return 1l;
    }

    public Set<String> getVariables() {
        return Collections.emptySet();
    }

    public <R> R get(final String variable) {
        return (R) this.giraphVertex.getConf().get(variable);
    }

    public void set(final String variable, Object value) {
        this.giraphVertex.getConf().set(variable, value.toString());
    }

    public void setIfAbsent(final String variable, final Object value) {
        this.giraphVertex.getConf().set(variable, value.toString());
    }

    public long incr(final String variable, final long delta) {
        return 1l;
    }

    public long decr(final String variable, final long delta) {
        return 1l;
    }

    public boolean and(final String variable, final boolean bool) {
        return true;
    }

    public boolean or(final String variable, final boolean bool) {
        return true;
    }

    public Graph getGraph() {
        return EmptyGraph.instance();
    }


}
