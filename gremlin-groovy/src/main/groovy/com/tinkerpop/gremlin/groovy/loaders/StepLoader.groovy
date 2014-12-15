package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.function.GComparator
import com.tinkerpop.gremlin.groovy.function.GFunction
import com.tinkerpop.gremlin.process.Step
import com.tinkerpop.gremlin.process.graph.GraphTraversal
import com.tinkerpop.gremlin.process.graph.marker.ByComparatorAcceptor
import com.tinkerpop.gremlin.process.graph.marker.FunctionRingAcceptor
import com.tinkerpop.gremlin.process.util.ByComparator
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

        GraphTraversal.metaClass.path = { final Closure... pathClosures ->
            return ((GraphTraversal) delegate).path(GFunction.make(pathClosures));
        }

        /*GraphTraversal.metaClass.by = { final Object... objects ->
            final Step end = TraversalHelper.getEnd((GraphTraversal) delegate);
            if (end instanceof FunctionRingAcceptor) {
                final Object[] newObjects = new Object[objects.length];
                for (int i = 0; i < objects.length; i++) {
                    if (objects[i] instanceof Closure) {
                        newObjects[i] = new GFunction((Closure) objects[i]);
                    } else {
                        newObjects[i] = objects[i];
                    }
                }
                ((FunctionRingAcceptor) end).setFunctionRing(new ByRing<>(newObjects));
            } else {
                final Comparator[] newObjects = new Comparator[objects.length];
                for (int i = 0; i < objects.length; i++) {
                    if (objects[i] instanceof Closure) {
                        newObjects[i] = new GComparator((Closure) objects[i]);
                    } else {
                        newObjects[i] = (Comparator) objects[i];
                    }
                }
                ((ByComparatorAcceptor) end).setByComparator(new ByComparator<>(newObjects[0]));
            }
            return (GraphTraversal) delegate;
        }*/

        GraphTraversal.metaClass.by = { final Closure closure ->
            final Step end = TraversalHelper.getEnd((GraphTraversal) delegate);
            if (end instanceof FunctionRingAcceptor) {
                ((FunctionRingAcceptor) end).setFunctionRing(new ByRing(new GFunction(closure)));
            } else {
                ((ByComparatorAcceptor) end).setByComparator(new ByComparator<>(new GComparator(closure)));
            }
            return (GraphTraversal) delegate;
        }

        GraphTraversal.metaClass.by = { final Closure closureA, final Closure closureB ->
            final Step end = TraversalHelper.getEnd((GraphTraversal) delegate);
            if (end instanceof FunctionRingAcceptor) {
                ((FunctionRingAcceptor) end).setFunctionRing(new ByRing(new GFunction(closureA), new GFunction(closureB)));
            } else {
                ((ByComparatorAcceptor) end).setByComparator(new ByComparator<>(new GComparator(closureA), new GComparator(closureB)));
            }
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
