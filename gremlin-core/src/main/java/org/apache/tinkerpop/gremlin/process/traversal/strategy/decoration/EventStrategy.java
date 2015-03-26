/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeByPathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventCallback;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.GraphChangedListener;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

/**
 * A strategy that raises events when {@link org.apache.tinkerpop.gremlin.process.traversal.step.Mutating} steps are
 * encountered and successfully executed.
 * <br/>
 * Note that this implementation requires a {@link Graph} on the {@link Traversal} instance.  If that is not present
 * an {@link java.lang.IllegalStateException} will be thrown.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class EventStrategy extends AbstractTraversalStrategy {
    private final List<GraphChangedListener> listeners = new ArrayList<>();

    private EventStrategy(final GraphChangedListener... listeners) {
        this.listeners.addAll(Arrays.asList(listeners));
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // EventStrategy requires access to both graph features and the current transaction object which are
        // both part of Graph - if that isn't present, this strategy doesn't work.
        if (!traversal.getGraph().isPresent())
            throw new IllegalStateException(String.format("%s requires a graph instance is present on the traversal", EventStrategy.class.getName()));

        final EventStrategyCallback callback = new EventStrategyCallback(new EventTrigger(traversal.getGraph().get()));
        TraversalHelper.getStepsOfAssignableClass(Mutating.class, traversal).forEach(s -> s.addCallback(callback));
    }

    public static Builder build() {
        return new Builder();
    }

    public class EventStrategyCallback implements EventCallback<Event>, Serializable {
        private final EventTrigger trigger;

        public EventStrategyCallback(final EventTrigger trigger) {
            this.trigger = trigger;
        }

        @Override
        public void accept(final Event event) {
            trigger.addEvent(event);
        }
    }

    public static class Builder {
        private final List<GraphChangedListener> listeners = new ArrayList<>();

        Builder() {}

        public Builder addListener(GraphChangedListener listener) {
            this.listeners.add(listener);
            return this;
        }

        public EventStrategy create() {
            return new EventStrategy(this.listeners.toArray(new GraphChangedListener[this.listeners.size()]));
        }
    }

    class EventTrigger {

        /**
         * A queue of events that are triggered by change to the graph. The queue builds up until the trigger fires them
         * in the order they were received.
         */
        private final ThreadLocal<Deque<Event>> eventQueue = new ThreadLocal<Deque<Event>>() {
            protected Deque<Event> initialValue() {
                return new ArrayDeque<>();
            }
        };

        /**
         * When set to true, events in the event queue will only be fired when a transaction is committed.
         */
        private final boolean enqueueEvents;

        public EventTrigger(final Graph graph) {
            enqueueEvents = graph.features().graph().supportsTransactions();

            if (enqueueEvents) {
                // since this is a transactional graph events are enqueued so the events should be fired/reset only after
                // transaction is committed/rolled back as tied to a graph transaction
                graph.tx().addTransactionListener(status -> {
                    if (status == Transaction.Status.COMMIT)
                        fireEventQueue();
                    else if (status == Transaction.Status.ROLLBACK)
                        resetEventQueue();
                    else
                        throw new RuntimeException(String.format("The %s is not aware of this status: %s", EventTrigger.class.getName(), status));
                });
            }
        }

        /**
         * Add an event to the event queue. If this is a non-transactional graph, then the queue fires and resets after
         * each event is added.
         */
        public void addEvent(final Event evt) {
            eventQueue.get().add(evt);

            if (!this.enqueueEvents) {
                fireEventQueue();
                resetEventQueue();
            }
        }

        private void resetEventQueue() {
            eventQueue.set(new ArrayDeque<>());
        }

        private void fireEventQueue() {
            final Deque<Event> deque = eventQueue.get();
            for (Event event = deque.pollFirst(); event != null; event = deque.pollFirst()) {
                event.fireEvent(listeners.iterator());
            }
        }
    }

}
