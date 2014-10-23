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

        final TraverserTracker tracker = vertex.value(TraversalVertexProgram.TRAVERSER_TRACKER);
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        final TraverserSet<Object> localTraverserSet = new TraverserSet<>();

        // process incoming traversers
        messenger.receiveMessages(MessageType.Global.to()).forEach(traverser -> {
            traverser.attach(vertex);
            localTraverserSet.add((Traverser.Admin) traverser);
        });

        while (!localTraverserSet.isEmpty()) {
            final TraverserSet<Object> tempTraverserSet = new TraverserSet<>();
            // process all the local objects and send messages or store locally accordingly
            localTraverserSet.forEach(traverser -> {
                if (traverser.get() instanceof Element || traverser.get() instanceof Property) {      // GRAPH OBJECT
                    if (traverser.isDone()) {
                        traverser.detach();
                        tracker.getDoneGraphTracks().add(traverser);
                    } else {
                        final Vertex hostingVertex = TraverserExecutor.getHostingVertex(traverser.get());
                        if (!vertex.equals(hostingVertex) || traverser.get() instanceof ReferencedElement) {
                            traverser.detach();
                            voteToHalt.set(false);
                            messenger.sendMessage(MessageType.Global.to(hostingVertex), traverser);
                        } else {
                            tempTraverserSet.add(traverser);
                        }
                    }
                } else {                                                                              // STANDARD OBJECT
                    if (traverser.isDone()) {
                        traverser.detach();
                        tracker.getDoneObjectTracks().add(traverser);
                    } else {
                        tempTraverserSet.add(traverser);
                    }
                }
            });
            localTraverserSet.clear();
            tempTraverserSet.forEach(start -> {
                final Step<?, ?> step = TraversalHelper.getStep(start.getFuture(), traversal);
                step.addStart((Traverser.Admin) start);
                step.forEachRemaining(end -> localTraverserSet.add((Traverser.Admin) end));
            });
        }
        return voteToHalt.get();
    }

    private final static Vertex getHostingVertex(final Object object) {
        if (object instanceof Vertex)
            return (Vertex) object;
        else if (object instanceof Edge) {
            return ((Edge) object).iterators().vertexIterator(Direction.OUT).next();
        } else if (object instanceof Property)
            return getHostingVertex(((Property) object).element());
        else
            throw new IllegalStateException("The host of the object is unknown: " + object.toString());
    }


}