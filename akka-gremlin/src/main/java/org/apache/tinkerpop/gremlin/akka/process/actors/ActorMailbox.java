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

package org.apache.tinkerpop.gremlin.akka.process.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Envelope;
import akka.dispatch.MailboxType;
import akka.dispatch.MessageQueue;
import akka.dispatch.ProducesMessageQueue;
import com.typesafe.config.Config;
import org.apache.tinkerpop.gremlin.process.actors.ActorsResult;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.util.ClassUtil;
import scala.Option;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ActorMailbox implements MailboxType, ProducesMessageQueue<ActorMailbox.ActorMessageQueue> {

    private final LinkedHashMap<Class, BinaryOperator> messageTypes = new LinkedHashMap<>();

    public static class ActorMessageQueue implements MessageQueue, ActorSemantics {
        private final Map<Class, BinaryOperator> messageTypes;
        private final List<Queue> messages;
        private ActorsResult result = null;
        private ActorRef home = null;
        private final Object MUTEX = new Object();


        public ActorMessageQueue(final Map<Class, BinaryOperator> messageTypes) {
            this.messageTypes = messageTypes;
            this.messages = new ArrayList<>(this.messageTypes.size());
            for (final Class clazz : this.messageTypes.keySet()) {
                final Queue queue;
                if (Traverser.class.isAssignableFrom(clazz))
                    queue = new TraverserSet<>();
                else
                    queue = new LinkedList<>();
                this.messages.add(queue);
            }
        }

        public void enqueue(final ActorRef receiver, final Envelope handle) {
            synchronized (MUTEX) {
                final Object message = handle.message();
                if (message instanceof ActorsResult) {
                    if (receiver.equals(handle.sender()))
                        this.result = (ActorsResult) message;
                    else
                        this.home = handle.sender();
                } else {
                    int i = 0;
                    for (final Map.Entry<Class, BinaryOperator> entry : this.messageTypes.entrySet()) {
                        final Class clazz = entry.getKey();
                        if (clazz.isInstance(message)) {
                            if (null != entry.getValue()) {
                                this.messages.get(i).offer(this.messages.get(i).isEmpty() ?
                                        message :
                                        entry.getValue().apply(this.messages.get(i).poll(), message));
                            } else
                                this.messages.get(i).offer(message);
                            return;
                        }
                        i++;
                    }
                    throw new IllegalArgumentException("The provided message type is not registered: " + handle.message().getClass());
                }
            }
        }

        public Envelope dequeue() {
            synchronized (MUTEX) {
                for (final Queue queue : this.messages) {
                    if (!queue.isEmpty()) {
                        return new Envelope(queue.poll(), ActorRef.noSender());
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
                this.home.tell(this.result, owner);
                for (final Queue queue : this.messages) {
                    while (!queue.isEmpty()) {
                        final Object m = queue.poll();
                        deadLetters.enqueue(owner, new Envelope(m, ActorRef.noSender()));
                    }
                }
            }
        }
    }

    // This constructor signature must exist, it will be called by Akka
    public ActorMailbox(final ActorSystem.Settings settings, final Config config) {
        final List<List<String>> messages = (List<List<String>>) config.getAnyRef("message-types");
        for (final List<String> message : messages) {
            this.messageTypes.put(
                    ClassUtil.getClassOrEnum(message.get(0)),
                    null == message.get(1) ?
                            null :
                            ClassUtil.getClassOrEnum(message.get(1)));
        }

    }

    // The create method is called to create the MessageQueue
    public MessageQueue create(final Option<ActorRef> owner, final Option<ActorSystem> system) {
        return new ActorMessageQueue(this.messageTypes);
    }

    public static interface ActorSemantics {

    }
}
