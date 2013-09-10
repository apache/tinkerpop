package com.tinkerpop.blueprints;

import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface OLAPGraph {

    public OLAPGraph operate(Consumer<Vertex> function);

    public OLAPGraph communicate(Consumer<Vertex> function);

}
