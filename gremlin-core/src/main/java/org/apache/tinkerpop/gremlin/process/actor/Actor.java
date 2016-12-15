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

package org.apache.tinkerpop.gremlin.process.actor;

import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.Partitioner;

import java.util.List;

/**
 * An Actor represents an isolated processing unit that can only be interacted with via messages.
 * Actors are able to send and receive messages. The {@link GraphActors} framework has two types of actors:
 * {@link Master} and {@link Worker}. A master actor is not associated with a particular graph {@link Partition}.
 * Instead, its role is to coordinate the workers and ultimately, yield the final result of the submitted
 * {@link ActorProgram}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Actor {

    /**
     * Get the {@link Partitioner} associated with the {@link GraphActors} system.
     *
     * @return the partitioner used to partition (logically and/or physically) the {@link org.apache.tinkerpop.gremlin.structure.Graph}
     */
    public Partitioner partitioner();

    /**
     * Get the {@link Address} of the actor.
     *
     * @return the actor's address
     */
    public Address address();

    /**
     * Get a list of the {@link Address} values of all the workers in {@link GraphActors} system.
     *
     * @return the worker's addresses
     */
    public List<Address.Worker> workers();

    /**
     * Send a message from this actor to another actor given their {@link Address}.
     *
     * @param toActor the actor to receive the messages
     * @param message the message being sent
     * @param <M>     the message type
     */
    public <M> void send(final Address toActor, final M message);

    public interface Master extends Actor {

        public Address.Master address();

        public void close();

        public <R> ActorsResult<R> result();

    }

    public interface Worker extends Actor {

        public Address.Worker address();

        public Address.Master master();

        /**
         * Get the {@link Partition} associated with this worker.
         * In principle, this is the subset of the {@link org.apache.tinkerpop.gremlin.structure.Graph} that
         * the worker is "data-local" to.
         *
         * @return the worker's partition
         */
        public Partition partition();
    }


}
