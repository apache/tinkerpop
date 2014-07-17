package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.GFunction
import com.tinkerpop.gremlin.groovy.GSupplier
import com.tinkerpop.gremlin.process.Traverser
import com.tinkerpop.gremlin.process.graph.GraphTraversal
import com.tinkerpop.gremlin.util.function.SFunction

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class StepLoader {

    public static void load() {

        GraphTraversal.metaClass.propertyMissing = { final String name ->
            if (GremlinLoader.isStep(name)) {
                return delegate."$name"();
            } else {
                return ((GraphTraversal) delegate).value(name);
            }
        }

        [Iterable, Iterator].each {
            it.metaClass.mean = {
                double counter = 0;
                double sum = 0;
                delegate.each { counter++; sum += it; }
                return sum / counter;
            }
        }

        GraphTraversal.metaClass.getAt = { final Integer index ->
            return ((GraphTraversal) delegate).range(index, index);
        }


        GraphTraversal.metaClass.getAt = { final Range range ->
            return ((GraphTraversal) delegate).range(range.getFrom() as Integer, range.getTo() as Integer);
        }

        // This allows the use to not specify get() to get at the object contained in the Traverser
        Traverser.metaClass.methodMissing = { final String name, final def args ->
            return ((Traverser) delegate).get()."$name"(*args);
        }

        Traverser.metaClass.propertyMissing = { final String name ->
            return ((Traverser) delegate).get()["$name"];
        }

        // THE CODE BELOW IS REQUIRED UNTIL GROOVY 2.3+ FIXES VAR ARG CONVERSION OF CLOSURES TO LAMBDAS

        GraphTraversal.metaClass.path = { final Closure... pathClosures ->
            return ((GraphTraversal) delegate).path(GFunction.make(pathClosures));
        }

        GraphTraversal.metaClass.select = { final List<String> asLabels ->
            return ((GraphTraversal) delegate).select(asLabels, new SFunction[0]);
        }

        GraphTraversal.metaClass.select = { final List<String> asLabels, final Closure... stepClosures ->
            return ((GraphTraversal) delegate).select(asLabels, GFunction.make(stepClosures));
        }

        GraphTraversal.metaClass.select = { final Closure... stepClosures ->
            return ((GraphTraversal) delegate).select(GFunction.make(stepClosures));
        }

        GraphTraversal.metaClass.aggregate = { final Closure... preAggregateClosures ->
            return ((GraphTraversal) delegate).aggregate(GFunction.make(preAggregateClosures));
        }

        GraphTraversal.metaClass.aggregate = { final String variable, final Closure... preAggregateClosures ->
            return ((GraphTraversal) delegate).aggregate(variable, GFunction.make(preAggregateClosures));
        }

        GraphTraversal.metaClass.aggregate = { final String variable ->
            return ((GraphTraversal) delegate).aggregate(variable, new SFunction[0]);
        }

        GraphTraversal.metaClass.groupCount = { final String variable, final Closure... preGroupClosures ->
            return ((GraphTraversal) delegate).groupCount(variable, GFunction.make(preGroupClosures));
        }

        GraphTraversal.metaClass.groupCount = { final Closure... preGroupClosures ->
            return ((GraphTraversal) delegate).groupCount(GFunction.make(preGroupClosures));
        }

        GraphTraversal.metaClass.tree = { final Closure... branchClosures ->
            return ((GraphTraversal) delegate).tree(GFunction.make(branchClosures));
        }

        GraphTraversal.metaClass.pageRank { final Closure closure ->
            final Closure newClosure = closure.dehydrate();
            return ((GraphTraversal) delegate).pageRank(new GSupplier(newClosure));
        }
    }
}
