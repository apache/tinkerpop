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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventCallback;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

/**
 * A strategy that raises events when {@link Mutating} steps are encountered and successfully executed.
 * <p/>
 * Note that this implementation requires a {@link Graph} on the {@link Traversal} instance.  If that is not present
 * an {@code IllegalStateException} will be thrown.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class EventStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {
    private final EventQueue eventQueue;
    private final Class<?> detachmentFactory;

    private EventStrategy(final Builder builder) {
        this.eventQueue = builder.eventQueue;
        this.eventQueue.setListeners(builder.listeners);
        this.detachmentFactory = builder.detachmentFactory;
    }

    public Class<?> getDetachmentFactory() {
        return this.detachmentFactory;
    }

    /**
     * Applies the appropriate detach operation to elements that will be raised in mutation events.
     */
    public <R> R detach(final R attached) {
        if (null == detachmentFactory)
            return attached;
        else if (detachmentFactory.equals(DetachedFactory.class))
            return DetachedFactory.detach(attached, true);
        else if (detachmentFactory.equals(ReferenceFactory.class))
            return ReferenceFactory.detach(attached);
        else
            throw new IllegalStateException("Unknown detachment option using " + detachmentFactory.getSimpleName());
    }

    /**
     * For newly created properties that do not yet exist, an empty {@link Property} is required that just contains
     * a key as a reference.
     */
    public <R extends Property> R empty(final Element element, final String key) {
        // currently the "no detachment" model simply returns a Detached value to maintain consistency with the
        // original API that already existed (where returning "Detached" was the only option). This could probably
        // change in the future to use an "empty" property or perhaps the "change" event API could change all together
        // and have a different return.
        if (null == detachmentFactory || detachmentFactory.equals(DetachedFactory.class)) {
            if (element instanceof Vertex)
                return (R) new DetachedVertexProperty(null, key, null, null);
            else
                return (R) new DetachedProperty(key, null);
        } else if (detachmentFactory.equals(ReferenceFactory.class)) {
            if (element instanceof Vertex)
                return (R) new ReferenceVertexProperty(new DetachedVertexProperty(null, key, null, null));
            else
                return (R) new ReferenceProperty(new DetachedProperty(key, null));
        } else
            throw new IllegalStateException("Unknown empty detachment option using " + detachmentFactory.getSimpleName());
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final EventStrategyCallback callback = new EventStrategyCallback(eventQueue);
        TraversalHelper.getStepsOfAssignableClass(Mutating.class, traversal).forEach(s -> s.getMutatingCallbackRegistry().addCallback(callback));
    }

    public static Builder build() {
        return new Builder();
    }

    public class EventStrategyCallback implements EventCallback<Event>, Serializable {
        private final EventQueue eventQueue;

        public EventStrategyCallback(final EventQueue eventQueue) {
            this.eventQueue = eventQueue;
        }

        @Override
        public void accept(final Event event) {
            eventQueue.addEvent(event);
        }
    }

    public final static class Builder {
        private final List<MutationListener> listeners = new ArrayList<>();
        private EventQueue eventQueue = new DefaultEventQueue();
        private Class<?> detachmentFactory = DetachedFactory.class;

        Builder() {}

        public Builder addListener(final MutationListener listener) {
            this.listeners.add(listener);
            return this;
        }

        public Builder eventQueue(final EventQueue eventQueue) {
            this.eventQueue = eventQueue;
            return this;
        }

        /**
         * Configures the method of detachment for element provided in mutation callback events. If configured with
         * {@code null} for no detachment with a transactional graph, be aware that accessing the evented elements
         * after {@code commit()} will likely open new transactions.
         *
         * @param factoryClass must be either {@code null} (for no detachment), {@link ReferenceFactory} for elements
         *                     with no properties or {@link DetachedFactory} for elements with properties.
         */
        public Builder detach(final Class<?> factoryClass) {
            detachmentFactory = factoryClass;
            return this;
        }

        public EventStrategy create() {
            return new EventStrategy(this);
        }
    }

    /**
     * Gathers messages from callbacks and fires them to listeners.  When the event is sent to the listener is
     * up to the implementation of this interface.
     */
    public interface EventQueue {
        /**
         * Provide listeners to the queue that were given to the {@link EventStrategy} on construction.
         */
        public void setListeners(final List<MutationListener> listeners);

        /**
         * Add an event to the event queue.
         */
        public void addEvent(final Event evt);
    }

    /**
     * Immediately notifies all listeners as events arrive.
     */
    public static class DefaultEventQueue implements EventQueue {
        private List<MutationListener> listeners = Collections.emptyList();

        @Override
        public void setListeners(final List<MutationListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void addEvent(final Event evt) {
            evt.fireEvent(listeners.iterator());
        }
    }

    /**
     * Stores events in a queue that builds up until the transaction is committed which then fires them in the order
     * they were received.
     */
    public static class TransactionalEventQueue implements EventQueue {

        private final ThreadLocal<Deque<Event>> eventQueue = new ThreadLocal<Deque<Event>>() {
            protected Deque<Event> initialValue() {
                return new ArrayDeque<>();
            }
        };

        private List<MutationListener> listeners = Collections.emptyList();

        public TransactionalEventQueue(final Graph graph) {
            if (!graph.features().graph().supportsTransactions())
                throw new IllegalStateException(String.format("%s requires the graph to support transactions", EventStrategy.class.getName()));

            // since this is a transactional graph events are enqueued so the events should be fired/reset only after
            // transaction is committed/rolled back as tied to a graph transaction
            graph.tx().addTransactionListener(status -> {
                if (status == Transaction.Status.COMMIT)
                    fireEventQueue();
                else if (status == Transaction.Status.ROLLBACK)
                    resetEventQueue();
                else
                    throw new RuntimeException(String.format("The %s is not aware of this status: %s", EventQueue.class.getName(), status));
            });
        }

        public void addEvent(final Event evt) {
            eventQueue.get().add(evt);
        }

        @Override
        public void setListeners(final List<MutationListener> listeners) {
            this.listeners = listeners;
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
