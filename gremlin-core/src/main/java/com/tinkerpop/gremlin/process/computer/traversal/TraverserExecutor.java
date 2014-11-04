package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraverserSet;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.referenced.ReferencedElement;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraverserExecutor {

    public static boolean execute(final Vertex vertex, final Messenger<Traverser.Admin<?>> messenger, final Traversal traversal) {

        final TraverserSet<Object> haltedTraversers = vertex.value(TraversalVertexProgram.HALTED_TRAVERSERS);
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);

        final TraverserSet<Object> aliveTraversers = new TraverserSet<>();
        // gather incoming traversers into a traverser set and gain the 'weighted-set' optimization
        messenger.receiveMessages(MessageType.Global.of()).forEach(traverser -> {
            traverser.attach(vertex);
            aliveTraversers.add((Traverser.Admin) traverser);
        });

        // while there are still local traversers, process them until they leave the vertex or halt (i.e. isHalted()).
        while (!aliveTraversers.isEmpty()) {
            final TraverserSet<Object> toProcessTraversers = new TraverserSet<>();
            // process all the local objects and send messages or store locally again
            aliveTraversers.forEach(traverser -> {
                if (traverser.get() instanceof Element || traverser.get() instanceof Property) {      // GRAPH OBJECT
                    // if the element is remote, then message, else store it locally for re-processing
                    final Vertex hostingVertex = TraverserExecutor.getHostingVertex(traverser.get());
                    if (!vertex.equals(hostingVertex) || traverser.get() instanceof ReferencedElement) {
                        voteToHalt.set(false);
                        traverser.detach();
                        messenger.sendMessage(MessageType.Global.of(hostingVertex), traverser);
                    } else
                        toProcessTraversers.add(traverser);
                } else                                                                              // STANDARD OBJECT
                    toProcessTraversers.add(traverser);
            });

            // process local traversers and if alive, repeat, else halt.
            aliveTraversers.clear();
            toProcessTraversers.forEach(start -> {
                final Step<?, ?> step = TraversalHelper.getStep(start.getFuture(), traversal);
                step.addStart((Traverser.Admin) start);
                step.forEachRemaining(end -> {
                    if (end.asAdmin().isHalted()) {
                        end.asAdmin().detach();
                        haltedTraversers.add((Traverser.Admin) end);
                    } else
                        aliveTraversers.add((Traverser.Admin) end);
                });
            });
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