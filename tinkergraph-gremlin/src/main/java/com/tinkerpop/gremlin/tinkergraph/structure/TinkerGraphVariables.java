package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.GraphVariableHelper;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphVariables implements Graph.Variables, Serializable {

    private final Graph graph;
    private final Map<String, Object> variables;

    public TinkerGraphVariables(final Graph graph) {
        this(graph, new ConcurrentHashMap<>());
    }

    public TinkerGraphVariables(final Graph graph, final Map<String, Object> state) {
        this.graph = graph;
        this.variables = state;
    }

    public Set<String> getVariables() {
        return this.variables.keySet();
    }

    public <R> R get(final String variable) {
        return (R) this.variables.get(variable);
    }

    public void set(final String variable, final Object value) {
        GraphVariableHelper.validateVariable(variable, value);
        this.variables.put(variable, value);
    }

    public Graph getGraph() {
        return this.graph;
    }
}
