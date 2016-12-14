/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.akka.process.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Envelope;
import akka.dispatch.MailboxType;
import akka.dispatch.MessageQueue;
import akka.dispatch.ProducesMessageQueue;
import com.typesafe.config.Config;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import scala.Option;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ActorMailbox implements MailboxType, ProducesMessageQueue<ActorMailbox.ActorMessageQueue> {

    private final List<Class> messagePriorities = new ArrayList<>();

    public static class ActorMessageQueue implements MessageQueue, ActorSemantics {
        private final List<Queue> messages;
        private final Map<Class, Queue> priorities;
        private final Object MUTEX = new Object();

        public ActorMessageQueue(final List<Class> messagePriorities) {
            this.messages = new ArrayList<>(messagePriorities.size());
            this.priorities = new HashMap<>(messagePriorities.size());
            for (final Class clazz : messagePriorities) {
                final Queue queue;
                if (Traverser.class.isAssignableFrom(clazz))
                    queue = new TraverserSet<>();
                else
                    queue = new LinkedList<>();
                this.messages.add(queue);
                this.priorities.put(clazz, queue);
            }
        }

        public void enqueue(final ActorRef receiver, final Envelope handle) {
            synchronized (MUTEX) {
                final Queue queue = this.priorities.get(handle.message() instanceof Traverser ? Traverser.class : handle.message().getClass());
                if (null == queue)
                    throw new IllegalArgumentException("The provided message type is not registered: " + handle.message().getClass());
                else
                    queue.offer(handle.message() instanceof Traverser ? handle.message() : handle);
            }
        }

        public Envelope dequeue() {
            synchronized (MUTEX) {
                for (final Queue queue : this.messages) {
                    if (!queue.isEmpty()) {
                        final Object m = queue.poll();
                        return m instanceof Traverser ? new Envelope(m, ActorRef.noSender()) : (Envelope) m;
                    }
                }
                return null;
            }
        }

        public int numberOfMessages() {
            synchronized (MUTEX) {
                int counter = 0;
                for (final Queue queue : this.messages) {
                    counter = counter + queue.size();
                }
                return counter;
            }
        }

        public boolean hasMessages() {
            synchronized (MUTEX) {
                for (final Queue queue : this.messages) {
                    if (!queue.isEmpty())
                        return true;
                }
                return false;
            }
        }

        public void cleanUp(final ActorRef owner, final MessageQueue deadLetters) {
            synchronized (MUTEX) {
                for (final Queue queue : this.messages) {
                    while (!queue.isEmpty()) {
                        final Object m = queue.poll();
                        deadLetters.enqueue(owner, m instanceof Traverser ? new Envelope(m, ActorRef.noSender()) : (Envelope) m);
                    }
                }
            }
        }
    }

    // This constructor signature must exist, it will be called by Akka
    public ActorMailbox(final ActorSystem.Settings settings, final Config config) {
        try {
            final String[] messages = ((String) settings.config().getAnyRef("message-priorities")).replace("[", "").replace("]", "").split(",");
            for (final String clazz : messages) {
                this.messagePriorities.add(Class.forName(clazz.trim()));
            }
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    // The create method is called to create the MessageQueue
    public MessageQueue create(final Option<ActorRef> owner, final Option<ActorSystem> system) {
        return new ActorMessageQueue(this.messagePriorities);
    }

    public static interface ActorSemantics {

    }
}
