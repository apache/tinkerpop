package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
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

    private Long counter;

    public TraversalCounterMessage() {

    }

    private TraversalCounterMessage(final Holder holder) {
        super(holder);
        this.counter = 1l;
    }

    public static TraversalCounterMessage of(final Holder holder) {
        return new TraversalCounterMessage(holder);
    }

    public Long getCounter() {
        return this.counter;
    }

    public void setCounter(final Long counter) {
        this.counter = counter;
    }

    public static boolean execute(final Vertex vertex,
                                  final Iterable<TraversalCounterMessage> messages,
                                  final Messenger messenger,
                                  final TraversalCounters tracker,
                                  final Traversal gremlin) {

        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        final Map<Holder, Long> localCounts = new HashMap<>();

        messages.forEach(message -> {
            message.holder.inflate(vertex);
            if (message.executeCounts(tracker, gremlin, localCounts))
                voteToHalt.set(false);
        });

        tracker.getPreviousObjectTracks().forEach((holder, counts) -> {
            if (holder.isDone()) {
                MapHelper.incr(tracker.getDoneObjectTracks(), holder, counts);
            } else {
                final Step step = TraversalHelper.getAs(holder.getFuture(), gremlin);
                for (int i = 0; i < counts; i++) {
                    step.addStarts(new SingleIterator(holder));
                }
                if (processStep(step, localCounts))
                    voteToHalt.set(false);
            }
        });

        localCounts.forEach((holder, count) -> {
            if (holder.get() instanceof Element || holder.get() instanceof Property) {
                final Object end = holder.get();
                final TraversalCounterMessage message = TraversalCounterMessage.of(holder);
                message.setCounter(count);
                messenger.sendMessage(
                        vertex,
                        MessageType.Global.of(TraversalVertexProgram.TRAVERSAL_MESSAGE, TraversalMessage.getHostingVertices(end)),
                        message);
            } else {
                MapHelper.incr(tracker.getObjectTracks(), holder, count);
            }
        });
        return voteToHalt.get();
    }

    private boolean executeCounts(final TraversalCounters tracker,
                                  final Traversal gremlin, Map<Holder, Long> localCounts) {

        if (this.holder.isDone()) {
            this.holder.deflate();
            MapHelper.incr(tracker.getDoneGraphTracks(), this.holder, this.counter);
            return false;
        }

        final Step step = TraversalHelper.getAs(this.holder.getFuture(), gremlin);
        MapHelper.incr(tracker.getGraphTracks(), this.holder, this.counter);
        for (int i = 0; i < this.counter; i++) {
            step.addStarts(new SingleIterator(this.holder));
        }
        return processStep(step, localCounts);
    }

    private static boolean processStep(final Step<?, ?> step, final Map<Holder, Long> localCounts) {
        final boolean messageSent = step.hasNext();
        step.forEachRemaining(holder -> MapHelper.incr(localCounts, holder, 1l));
        return messageSent;
    }
}


