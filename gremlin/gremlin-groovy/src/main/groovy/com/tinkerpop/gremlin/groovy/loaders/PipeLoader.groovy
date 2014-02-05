package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.GremlinLoader
import com.tinkerpop.gremlin.process.Traversal

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class PipeLoader {

    public static void load() {

        Traversal.metaClass.propertyMissing = { final String name ->
            if (GremlinLoader.isStep(name)) {
                return delegate."$name"();
            } else {
                return ((Traversal) delegate).value(name);
            }
        }

        /*[Iterable, Iterator].each {
            it.metaClass.count = {
                return PipeHelper.counter(delegate.iterator());
            }
        }*/

        [Iterable, Iterator].each {
            it.metaClass.mean = {
                double counter = 0;
                double sum = 0;
                delegate.each { counter++; sum += it; }
                return sum / counter;
            }
        }

        Traversal.metaClass.getAt = { final Integer index ->
            return ((Traversal) delegate).range(index, index);
        }


        Traversal.metaClass.getAt = { final Range range ->
            return ((Traversal) delegate).range(range.getFrom() as Integer, range.getTo() as Integer);
        }
    }
}
