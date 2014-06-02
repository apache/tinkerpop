package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.structure.Property;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface SideEffectCapable {

    public static final String CAP_VARIABLE = Property.hidden("cap");
}
