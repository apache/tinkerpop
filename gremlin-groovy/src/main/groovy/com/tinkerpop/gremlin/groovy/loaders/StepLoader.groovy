package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.function.GComparator
import com.tinkerpop.gremlin.groovy.function.GFunction
import com.tinkerpop.gremlin.process.graph.GraphTraversal

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class StepLoader {

    public static void load() {

        [Iterable, Iterator].each {
            it.metaClass.mean = {
                double counter = 0;
                double sum = 0;
                delegate.each { counter++; sum += it; }
                return sum / counter;
            }
        }

        GraphTraversal.metaClass.by = { final Closure closure ->
            return ((GraphTraversal) delegate).by(1 == closure.getMaximumNumberOfParameters() ? new GFunction(closure) : new GComparator(closure));
        }
    }
}
