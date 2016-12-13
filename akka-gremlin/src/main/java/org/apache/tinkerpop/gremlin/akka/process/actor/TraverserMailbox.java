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
import org.apache.tinkerpop.gremlin.process.actor.traversal.TraversalWorkerProgram;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.VoteToHaltMessage;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import scala.Option;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraverserMailbox implements MailboxType, ProducesMessageQueue<TraverserMailbox.TraverserMessageQueue> {

    public static class TraverserMessageQueue implements MessageQueue, TraverserSetSemantics {
        private final Queue<Envelope> otherMessages = new LinkedList<>();
        private final TraverserSet<?> traverserMessages = new TraverserSet<>();
        private Envelope haltMessage = null;
        private Envelope terminateToken = null;
        private final ActorRef owner;
        private final Object MUTEX = new Object();

        public TraverserMessageQueue(final ActorRef owner) {
            this.owner = owner;
        }

        public void enqueue(final ActorRef receiver, final Envelope handle) {
            synchronized (MUTEX) {
                if (handle.message() instanceof Traverser.Admin)
                    this.traverserMessages.offer((Traverser.Admin) handle.message());
                else if (handle.message() instanceof VoteToHaltMessage) {
                    assert null == this.haltMessage;
                    this.haltMessage = handle;
                } else if (handle.message() instanceof TraversalWorkerProgram.Terminate) {
                    assert null == this.terminateToken;
                    this.terminateToken = handle;
                } else
                    this.otherMessages.offer(handle);
            }
        }

        public Envelope dequeue() {
            synchronized (MUTEX) {
                if (!this.otherMessages.isEmpty())
                    return this.otherMessages.poll();
                if (!this.traverserMessages.isEmpty())
                    return new Envelope(this.traverserMessages.poll(), this.owner);
                else if (null != this.terminateToken) {
                    final Envelope temp = this.terminateToken;
                    this.terminateToken = null;
                    return temp;
                } else {
                    final Envelope temp = this.haltMessage;
                    this.haltMessage = null;
                    return temp;
                }
            }
        }

        public int numberOfMessages() {
            synchronized (MUTEX) {
                return this.otherMessages.size() + this.traverserMessages.size() + (null == this.haltMessage ? 0 : 1) + (null == this.terminateToken ? 0 : 1);
            }
        }

        public boolean hasMessages() {
            synchronized (MUTEX) {
                return !this.otherMessages.isEmpty() || !this.traverserMessages.isEmpty() || null != this.haltMessage || this.terminateToken != null;
            }
        }

        public void cleanUp(final ActorRef owner, final MessageQueue deadLetters) {
            synchronized (MUTEX) {
                for (final Envelope handle : this.otherMessages) {
                    deadLetters.enqueue(owner, handle);
                }
                for (final Traverser.Admin<?> traverser : this.traverserMessages) {
                    deadLetters.enqueue(owner, new Envelope(traverser, this.owner));
                }
                if (null != this.haltMessage) {
                    deadLetters.enqueue(owner, this.haltMessage);
                    this.haltMessage = null;
                }
                if (null != this.terminateToken) {
                    deadLetters.enqueue(owner, this.terminateToken);
                    this.terminateToken = null;
                }
            }
        }
    }

    // This constructor signature must exist, it will be called by Akka
    public TraverserMailbox(final ActorSystem.Settings settings, final Config config) {
        // put your initialization code here
    }

    // The create method is called to create the MessageQueue
    public MessageQueue create(final Option<ActorRef> owner, final Option<ActorSystem> system) {
        return new TraverserMessageQueue(owner.isEmpty() ? ActorRef.noSender() : owner.get());
    }

    public static interface TraverserSetSemantics {

    }
}
