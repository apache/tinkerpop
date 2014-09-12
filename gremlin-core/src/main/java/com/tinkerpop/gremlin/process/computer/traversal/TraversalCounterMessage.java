package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SSupplier;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalCounterMessage extends TraversalMessage {

    private Long counter;

    private TraversalCounterMessage() {
    }

    private TraversalCounterMessage(final Traverser.System traverser) {
        super(traverser);
        this.counter = 1l;
    }

    public static TraversalCounterMessage of(final Traverser.System traverser) {
        return new TraversalCounterMessage(traverser);
    }

    public Long getCounter() {
        return this.counter;
    }

    public void setCounter(final Long counter) {
        this.counter = counter;
    }

    public static boolean execute(final Vertex vertex, final Messenger messenger, final SSupplier<Traversal> traversalSupplier) {

        final TraverserCountTracker tracker = vertex.value(TraversalVertexProgram.TRAVERSER_TRACKER);
        final Traversal traversal = traversalSupplier.get();
        traversal.strategies().apply();

        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        final Map<Traverser, Long> localCounts = new HashMap<>();

        messenger.receiveMessages(MessageType.Global.of()).forEach(message -> {
            ((TraversalCounterMessage) message).traverser.inflate(vertex,traversal);
            if (((TraversalCounterMessage) message).executeCounts(tracker, traversal, localCounts, vertex))
                voteToHalt.set(false);
        });

        tracker.getPreviousObjectTracks().forEach((traverser, counts) -> {
            if (traverser.isDone()) {
                MapHelper.incr(tracker.getDoneObjectTracks(), traverser, counts);
            } else {
                final Step step = TraversalHelper.getStep(traverser.getFuture(), traversal);
                if (step instanceof VertexCentric) ((VertexCentric) step).setCurrentVertex(vertex);
                if (step instanceof Bulkable) ((Bulkable) step).setCurrentBulkCount(counts);
                step.addStarts(new SingleIterator(traverser));
                if (processStep(step, localCounts, counts))
                    voteToHalt.set(false);
            }
        });

        localCounts.forEach((traverser, count) -> {
            if (traverser.get() instanceof Element || traverser.get() instanceof Property) {
                final Object end = traverser.get();
                final TraversalCounterMessage message = TraversalCounterMessage.of((Traverser.System) traverser);
                message.setCounter(count);
                messenger.sendMessage(
                        MessageType.Global.of(TraversalMessage.getHostingVertices(end)),
                        message);
            } else {
                MapHelper.incr(tracker.getObjectTracks(), (Traverser.System) traverser, count);
            }
        });
        return voteToHalt.get();
    }

    private boolean executeCounts(final TraverserCountTracker tracker,
                                  final Traversal traversal, Map<Traverser, Long> localCounts,
                                  final Vertex vertex) {

        if (this.traverser.isDone()) {
            this.traverser.deflate();
            MapHelper.incr(tracker.getDoneGraphTracks(), this.traverser, this.counter);
            return false;
        }

        final Step step = TraversalHelper.getStep(this.traverser.getFuture(), traversal);
        MapHelper.incr(tracker.getGraphTracks(), this.traverser, this.counter);

        if (step instanceof VertexCentric) ((VertexCentric) step).setCurrentVertex(vertex);
        if (step instanceof Bulkable) ((Bulkable) step).setCurrentBulkCount(this.counter);

        step.addStarts(new SingleIterator(this.traverser));
        return processStep(step, localCounts, this.counter);
    }

    private static boolean processStep(final Step<?, ?> step, final Map<Traverser, Long> localCounts, final long counter) {
        final boolean messageSent = step.hasNext();
        step.forEachRemaining(traverser -> {
            MapHelper.incr(localCounts, traverser, counter);
        });
        return messageSent;
    }
}


