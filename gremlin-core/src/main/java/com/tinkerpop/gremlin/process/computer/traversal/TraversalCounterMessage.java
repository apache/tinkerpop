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
public class TraversalCounterMessage extends TraversalMessage {


    private TraversalCounterMessage() {
    }

    private TraversalCounterMessage(final Traverser.System traverser) {
        super(traverser);
    }

    public static TraversalCounterMessage of(final Traverser.System traverser) {
        return new TraversalCounterMessage(traverser);
    }

    public static boolean execute(final Vertex vertex, final Messenger messenger, final Traversal traversal) {

        final TraverserCountTracker tracker = vertex.value(TraversalVertexProgram.TRAVERSER_TRACKER);
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        final Map<Traverser.System, Long> localCounts = new HashMap<>();

        messenger.receiveMessages(MessageType.Global.of()).forEach(message -> {
            ((TraversalCounterMessage) message).traverser.inflate(vertex, traversal);
            if (((TraversalCounterMessage) message).executeCounts(tracker, traversal, localCounts))
                voteToHalt.set(false);
        });

        tracker.getPreviousObjectTracks().keySet().forEach(traverser -> {
            if (traverser.isDone()) {
                MapHelper.incr(tracker.getDoneObjectTracks(), traverser, traverser.getBulk());
            } else {
                traverser.inflate(vertex, traversal);
                final Step step = TraversalHelper.getStep(traverser.getFuture(), traversal);
                step.addStarts(new SingleIterator(traverser));
                if (processStep(step, localCounts))
                    voteToHalt.set(false);
            }
        });

        localCounts.forEach((traverser, count) -> {
            traverser.setBulk(count);
            if (traverser.get() instanceof Element || traverser.get() instanceof Property) {
                final Object end = traverser.get();
                final TraversalCounterMessage message = TraversalCounterMessage.of(traverser);
                messenger.sendMessage(
                        MessageType.Global.of(TraversalMessage.getHostingVertex(end)),
                        message);
            } else {
                traverser.deflate();
                MapHelper.incr(tracker.getObjectTracks(), traverser, traverser.getBulk());
            }
        });
        return voteToHalt.get();
    }

    private boolean executeCounts(final TraverserCountTracker tracker,
                                  final Traversal traversal,
                                  final Map<Traverser.System, Long> localCounts) {

        if (this.traverser.isDone()) {
            this.traverser.deflate();
            MapHelper.incr(tracker.getDoneGraphTracks(), this.traverser, this.traverser.getBulk());
            return false;
        }

        final Step step = TraversalHelper.getStep(this.traverser.getFuture(), traversal);
        step.addStarts(new SingleIterator(this.traverser));
        return processStep(step, localCounts);
    }

    private static boolean processStep(final Step<?, ?> step, final Map<Traverser.System, Long> localCounts) {
        final boolean messageSent = step.hasNext();
        step.forEachRemaining(traverser -> MapHelper.incr(localCounts, (Traverser.System) traverser, traverser.getBulk()));
        return messageSent;
    }
}

