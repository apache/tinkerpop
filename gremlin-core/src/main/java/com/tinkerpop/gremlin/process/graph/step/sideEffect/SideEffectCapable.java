package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface SideEffectCapable {

    public static final String CAP_VARIABLE = Graph.Key.hidden("cap");
}
