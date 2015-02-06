package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MessageScope;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.traversal.TraversalMatrix;
import com.tinkerpop.gremlin.process.util.TraverserSet;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.detached.DetachedElement;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraverserExecutor {

    public static boolean execute(final Vertex vertex, final Messenger<TraverserSet<?>> messenger, final TraversalMatrix<?, ?> traversalMatrix) {

        final TraverserSet<Object> haltedTraversers = vertex.value(TraversalVertexProgram.HALTED_TRAVERSERS);
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);

        final TraverserSet<Object> aliveTraversers = new TraverserSet<>();
        // gather incoming traversers into a traverser set and gain the 'weighted-set' optimization
        final TraversalSideEffects traversalSideEffects = traversalMatrix.getTraversal().getSideEffects();
        messenger.receiveMessages(MessageScope.Global.instance()).forEach(traverserSet -> {
            traverserSet.forEach(traverser -> {
                traverser.setSideEffects(traversalSideEffects);
                traverser.attach(vertex);
                aliveTraversers.add((Traverser.Admin) traverser);
            });
        });

        // while there are still local traversers, process them until they leave the vertex or halt (i.e. isHalted()).
        final TraverserSet<Object> toProcessTraversers = new TraverserSet<>();
        while (!aliveTraversers.isEmpty()) {
            // process all the local objects and send messages or store locally again
            aliveTraversers.forEach(traverser -> {
                if (traverser.get() instanceof Element || traverser.get() instanceof Property) {      // GRAPH OBJECT
                    // if the element is remote, then message, else store it locally for re-processing
                    final Vertex hostingVertex = TraverserExecutor.getHostingVertex(traverser.get());
                    if (!vertex.equals(hostingVertex) || traverser.get() instanceof DetachedElement) { // TODO: why is the DetachedElement instanceof needed?
                        voteToHalt.set(false);
                        traverser.detach();
                        messenger.sendMessage(MessageScope.Global.of(hostingVertex), new TraverserSet<>(traverser));
                    } else
                        toProcessTraversers.add(traverser);
                } else                                                                              // STANDARD OBJECT
                    toProcessTraversers.add(traverser);
            });

            // process local traversers and if alive, repeat, else halt.
            aliveTraversers.clear();
            toProcessTraversers.forEach(start -> {
                final Step<?, ?> step = traversalMatrix.getStepById(start.getStepId());
                step.addStart((Traverser.Admin) start);
                step.forEachRemaining(end -> {
                    if (end.asAdmin().isHalted()) {
                        end.asAdmin().detach();
                        haltedTraversers.add((Traverser.Admin) end);
                    } else
                        aliveTraversers.add((Traverser.Admin) end);
                });
            });

            toProcessTraversers.clear();
        }
        return voteToHalt.get();
    }

    private final static Vertex getHostingVertex(final Object object) {
        if (object instanceof Vertex)
            return (Vertex) object;
        else if (object instanceof Edge)
            return ((Edge) object).iterators().vertexIterator(Direction.OUT).next();
        else if (object instanceof Property)
            return getHostingVertex(((Property) object).element());
        else
            throw new IllegalStateException("The host of the object is unknown: " + object.toString() + ":" + object.getClass().getCanonicalName());
    }
}