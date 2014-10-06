package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.structure.util.HasContainer;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface HasContainerHolder {

    public List<HasContainer> getHasContainers();
}
