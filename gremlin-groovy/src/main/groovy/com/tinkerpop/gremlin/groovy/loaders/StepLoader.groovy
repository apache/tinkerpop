package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.function.*
import com.tinkerpop.gremlin.process.graph.GraphTraversal

import java.util.function.Function

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class StepLoader {

    public void loadIt() {

        [Iterable, Iterator].each {
            it.metaClass.mean = {
                double counter = 0;
                double sum = 0;
                delegate.each { counter++; sum += it; }
                return sum / counter;
            }
        }

        // THE CODE BELOW IS REQUIRED UNTIL GROOVY 2.3+ FIXES VAR ARG CONVERSION OF CLOSURES TO LAMBDAS

        GraphTraversal.metaClass.path = { final Closure... pathClosures ->
            return ((GraphTraversal) delegate).path(GFunction.make(pathClosures));
        }

        GraphTraversal.metaClass.select = { final List<String> asLabels ->
            return ((GraphTraversal) delegate).select(asLabels, new Function[0]);
        }

        GraphTraversal.metaClass.select = { final List<String> asLabels, final Closure... stepClosures ->
            return ((GraphTraversal) delegate).select(asLabels, GFunction.make(stepClosures));
        }

        GraphTraversal.metaClass.select = { final Closure... stepClosures ->
            return ((GraphTraversal) delegate).select(GFunction.make(stepClosures));
        }

        GraphTraversal.metaClass.tree = { final String memoryKey ->
            return ((GraphTraversal) delegate).tree(memoryKey, new Function[0]);
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

    public static void load() {

        [Iterable, Iterator].each {
            it.metaClass.mean = {
                double counter = 0;
                double sum = 0;
                delegate.each { counter++; sum += it; }
                return sum / counter;
            }
        }

        // THE CODE BELOW IS REQUIRED UNTIL GROOVY 2.3+ FIXES VAR ARG CONVERSION OF CLOSURES TO LAMBDAS

        GraphTraversal.metaClass.path = { final Closure... pathClosures ->
            return ((GraphTraversal) delegate).path(GFunction.make(pathClosures));
        }

        GraphTraversal.metaClass.select = { final List<String> asLabels ->
            return ((GraphTraversal) delegate).select(asLabels, new Function[0]);
        }

        GraphTraversal.metaClass.select = { final List<String> asLabels, final Closure... stepClosures ->
            return ((GraphTraversal) delegate).select(asLabels, GFunction.make(stepClosures));
        }

        GraphTraversal.metaClass.select = { final Closure... stepClosures ->
            return ((GraphTraversal) delegate).select(GFunction.make(stepClosures));
        }

        GraphTraversal.metaClass.tree = { final String memoryKey ->
            return ((GraphTraversal) delegate).tree(memoryKey, new Function[0]);
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
