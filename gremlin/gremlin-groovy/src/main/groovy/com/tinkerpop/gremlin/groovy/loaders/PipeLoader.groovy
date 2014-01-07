package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.GremlinLoader
import com.tinkerpop.gremlin.pipes.GremlinPipeline

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class PipeLoader {

    public static void load() {

        GremlinPipeline.metaClass.propertyMissing = { final String name ->
            if (GremlinLoader.isStep(name)) {
                return delegate."$name"();
            } else {
                return ((GremlinPipeline) delegate).value(name);
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

        GremlinPipeline.metaClass.getAt = { final Integer index ->
            return ((GremlinPipeline) delegate).range(index, index);
        }


        GremlinPipeline.metaClass.getAt = { final Range range ->
            return ((GremlinPipeline) delegate).range(range.getFrom() as Integer, range.getTo() as Integer);
        }
    }
}
