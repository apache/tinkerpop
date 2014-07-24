package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface SideEffectCapable {

    public static final String CAP_KEY = Graph.Key.hidden("cap");

    public String getVariable();

}
