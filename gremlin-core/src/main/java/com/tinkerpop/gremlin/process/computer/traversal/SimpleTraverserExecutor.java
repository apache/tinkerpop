package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SimpleTraverserExecutor extends TraverserExecutor {

    public static boolean execute(final Vertex vertex, final Messenger<Traverser.Admin<?>> messenger, final Traversal traversal) {

        final TraverserCountTracker tracker = vertex.value(TraversalVertexProgram.TRAVERSER_TRACKER);
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        final Map<Traverser.Admin, Long> localCounts = new HashMap<>();

        // process incoming traversers
        messenger.receiveMessages(MessageType.Global.to()).forEach(traverser -> {
            if (traverser.isDone()) {
                // no need to deflate as they are already deflated
                MapHelper.incr(tracker.getDoneGraphTracks(), traverser, traverser.getBulk());
            } else {
                traverser.inflate(vertex, traversal);
                final Step<?, ?> step = TraversalHelper.getStep(traverser.getFuture(), traversal);
                step.addStarts(new SingleIterator(traverser));
                if (processStep(step, localCounts))
                    voteToHalt.set(false);
            }
        });

        // process existing traversers that reference local objects
        tracker.getPreviousObjectTracks().keySet().forEach(traverser -> {
            if (traverser.isDone()) {
                // no need to deflate as they are already deflated
                MapHelper.incr(tracker.getDoneObjectTracks(), traverser, traverser.getBulk());
            } else {
                traverser.inflate(vertex, traversal);
                final Step<?, ?> step = TraversalHelper.getStep(traverser.getFuture(), traversal);
                step.addStarts(new SingleIterator(traverser));
                if (processStep(step, localCounts))
                    voteToHalt.set(false);
            }
        });

        // process all the local object and send messages or store locally accordingly
        localCounts.forEach((traverser, count) -> {
            traverser.setBulk(count);
            traverser.deflate();
            if (traverser.get() instanceof Element || traverser.get() instanceof Property)
                messenger.sendMessage(MessageType.Global.to(TraverserExecutor.getHostingVertex(traverser.get())), traverser);
            else
                MapHelper.incr(tracker.getObjectTracks(), traverser, traverser.getBulk());

        });
        return voteToHalt.get();
    }

    private final static boolean processStep(final Step<?, ?> step, final Map<Traverser.Admin, Long> localCounts) {
        final boolean messageSent = step.hasNext();
        step.forEachRemaining(traverser -> MapHelper.incr(localCounts, traverser.asAdmin(), traverser.getBulk()));
        return messageSent;
    }
}

