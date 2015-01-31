package com.tinkerpop.gremlin.process.graph.traversal.step;

import com.tinkerpop.gremlin.process.graph.util.HasContainer;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface HasContainerHolder {

    public List<HasContainer> getHasContainers();
}
