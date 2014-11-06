package com.tinkerpop.gremlin.process.traversers;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraverserGeneratorFactory {

    public TraverserGenerator getTraverserGenerator(final Traversal traversal);
}
