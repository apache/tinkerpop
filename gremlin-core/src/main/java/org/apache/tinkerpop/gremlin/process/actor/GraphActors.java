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

import org.apache.tinkerpop.gremlin.process.Processor;
import org.apache.tinkerpop.gremlin.structure.Partitioner;

import java.util.concurrent.Future;

/**
 * GraphActors is a message-passing based graph {@link Processor} that is:
 * asynchronous, distributed, and partition centric.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphActors<R> extends Processor {

    /**
     * Provide the {@link ActorProgram} that the GraphActors will execute.
     *
     * @param program the program to execute
     * @return the updated GraphActors with newly defined program
     */
    public GraphActors<R> program(final ActorProgram<R> program);

    /**
     * Provide the {@link Partitioner} that the GraphActors will execute over.
     * Typically, there will be a single {@link org.apache.tinkerpop.gremlin.process.actor.Actor.Worker}
     * for each {@link org.apache.tinkerpop.gremlin.structure.Partition} in the partitioner.
     *
     * @param partitioner the partitioner defining the data partitions
     * @return the updated GraphActors with newly defined partitioner
     */
    public GraphActors<R> partitioner(final Partitioner partitioner);

    /**
     * Submit the {@link ActorProgram} for execution by the {@link GraphActors}.
     *
     * @return a {@link Future} denoting a reference to the asynchronous computation's result
     */
    public Future<R> submit();
}
