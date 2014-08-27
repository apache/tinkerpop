package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.function.*
import com.tinkerpop.gremlin.process.graph.GraphTraversal
import com.tinkerpop.gremlin.structure.Element
import com.tinkerpop.gremlin.util.function.SFunction

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

        GraphTraversal.metaClass.getAt = { final Integer index ->
            return ((GraphTraversal) delegate).range(index, index);
        }

        GraphTraversal.metaClass.getAt = { final Range range ->
            return ((GraphTraversal) delegate).range(range.getFrom() as Integer, range.getTo() as Integer);
        }

        // THE CODE BELOW IS REQUIRED UNTIL GROOVY 2.3+ FIXES VAR ARG CONVERSION OF CLOSURES TO LAMBDAS

        GraphTraversal.metaClass.map = { final Closure closure ->
            return 1 == closure.getMaximumNumberOfParameters() ?
                    ((GraphTraversal) delegate).map(new GFunction<>(closure)) :
                    ((GraphTraversal) delegate).map(new GBiFunction<>(closure))
        }

        GraphTraversal.metaClass.flatMap = { final Closure closure ->
            return 1 == closure.getMaximumNumberOfParameters() ?
                    ((GraphTraversal) delegate).flatMap(new GFunction<>(closure)) :
                    ((GraphTraversal) delegate).flatMap(new GBiFunction<>(closure))
        }

        GraphTraversal.metaClass.filter = { final Closure closure ->
            return 1 == closure.getMaximumNumberOfParameters() ?
                    ((GraphTraversal) delegate).filter(new GPredicate<>(closure)) :
                    ((GraphTraversal) delegate).filter(new GBiPredicate<>(closure))
        }

        GraphTraversal.metaClass.sideEffect = { final Closure closure ->
            return 1 == closure.getMaximumNumberOfParameters() ?
                    ((GraphTraversal) delegate).sideEffect(new GConsumer<>(closure)) :
                    ((GraphTraversal) delegate).sideEffect(new GBiConsumer<>(closure))
        }

        Element.metaClass.map = { final Closure closure ->
            return 1 == closure.getMaximumNumberOfParameters() ?
                    delegate.map(new GFunction<>(closure)) :
                    delegate.map(new GBiFunction<>(closure))
        }

        Element.metaClass.flatMap = { final Closure closure ->
            return 1 == closure.getMaximumNumberOfParameters() ?
                    delegate.flatMap(new GFunction<>(closure)) :
                    delegate.flatMap(new GBiFunction<>(closure))
        }

        Element.metaClass.filter = { final Closure closure ->
            return 1 == closure.getMaximumNumberOfParameters() ?
                    delegate.filter(new GPredicate<>(closure)) :
                    delegate.filter(new GBiPredicate<>(closure))
        }

        Element.metaClass.sideEffect = { final Closure closure ->
            return 1 == closure.getMaximumNumberOfParameters() ?
                    delegate.sideEffect(new GConsumer<>(closure)) :
                    delegate.sideEffect(new GBiConsumer<>(closure))
        }

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

        GraphTraversal.metaClass.tree = { final String memoryKey ->
            return ((GraphTraversal) delegate).tree(memoryKey, new SFunction[0]);
        }

        GraphTraversal.metaClass.tree = { final Closure... branchClosures ->
            return ((GraphTraversal) delegate).tree(GFunction.make(branchClosures));
        }

        GraphTraversal.metaClass.tree = { final String memoryKey, final Closure... branchClosures ->
            return ((GraphTraversal) delegate).tree(memoryKey, GFunction.make(branchClosures));
        }

        GraphTraversal.metaClass.pageRank { final Closure closure ->
            final Closure newClosure = closure.dehydrate();
            return ((GraphTraversal) delegate).pageRank(new GSupplier(newClosure));
        }
    }
}
