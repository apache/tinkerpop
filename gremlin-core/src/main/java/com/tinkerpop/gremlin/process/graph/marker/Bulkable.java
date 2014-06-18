package com.tinkerpop.gremlin.process.graph.marker;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Bulkable {

    public void setCurrentBulkCount(final long count);
}
