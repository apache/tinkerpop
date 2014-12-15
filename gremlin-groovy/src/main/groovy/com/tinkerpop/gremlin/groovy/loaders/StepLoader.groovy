package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.function.GBinaryOperator
import com.tinkerpop.gremlin.groovy.function.GComparator
import com.tinkerpop.gremlin.groovy.function.GFunction
import com.tinkerpop.gremlin.groovy.function.GUnaryOperator
import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.graph.GraphTraversal
import com.tinkerpop.gremlin.process.graph.marker.FunctionRingAcceptor
import com.tinkerpop.gremlin.process.util.ByRing
import com.tinkerpop.gremlin.process.util.TraversalHelper

import java.util.function.Function

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

        // THE CODE BELOW IS REQUIRED UNTIL GROOVY 2.3+ FIXES VAR ARG CONVERSION OF CLOSURES TO LAMBDAS

        GraphTraversal.metaClass.branch = { final Closure... labelClosures ->
            return ((GraphTraversal) delegate).branch(GFunction.make(labelClosures));
        }

        GraphTraversal.metaClass.order = { final Closure... orderClosures ->
            return ((GraphTraversal) delegate).order(GComparator.make(orderClosures));
        }

        GraphTraversal.metaClass.path = { final Closure... pathClosures ->
            return ((GraphTraversal) delegate).path(GFunction.make(pathClosures));
        }

        GraphTraversal.metaClass.by = { final Object... objects ->
            final Object[] newObjects = new Object[objects.length];
            for (int i = 0; i < objects.length; i++) {
                if (objects[i] instanceof Closure) {
                    newObjects[i] = new GFunction((Closure) objects[i]);
                } else {
                    newObjects[i] = objects[i];
                }
            }
            ((FunctionRingAcceptor) TraversalHelper.getEnd((GraphTraversal) delegate)).setFunctionRing(new ByRing<>(newObjects));
            return (GraphTraversal) delegate;
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
    }
}
