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

import akka.actor.ActorSystem;
import akka.actor.Props;
import org.apache.tinkerpop.gremlin.process.actor.ActorProgram;
import org.apache.tinkerpop.gremlin.process.actor.Actors;
import org.apache.tinkerpop.gremlin.process.actor.Address;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Partitioner;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AkkaActors<S, E> implements Actors<S, E> {

    private final ActorProgram actorProgram;
    private final ActorSystem system;
    private final Address.Master master;

    public AkkaActors(final ActorProgram actorProgram, final Partitioner partitioner) {
        this.actorProgram = actorProgram;
        this.system = ActorSystem.create("traversal-" + actorProgram.hashCode());
        this.master = new Address.Master(this.system.actorOf(Props.create(MasterTraversalActor.class, this.actorProgram, partitioner), "master").path().toString());
    }

    @Override
    public Address.Master master() {
        return this.master;
    }

    @Override
    public Future<TraverserSet<E>> submit() {
        return CompletableFuture.supplyAsync(() -> {
            while (!this.system.isTerminated()) {

            }
            return (TraverserSet) this.actorProgram.getResult();
        });
    }
}

