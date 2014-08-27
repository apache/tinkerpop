package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.computer.GraphComputer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphComputerAnalyzer {

    public void registerGraphComputer(final GraphComputer graphComputer);
}
