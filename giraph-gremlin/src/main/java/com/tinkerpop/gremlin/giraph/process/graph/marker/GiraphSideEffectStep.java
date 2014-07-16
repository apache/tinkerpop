package com.tinkerpop.gremlin.giraph.process.graph.marker;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GiraphSideEffectStep<T> {

    public T getSideEffect(final Configuration configuration);

    public String getVariable();
}
