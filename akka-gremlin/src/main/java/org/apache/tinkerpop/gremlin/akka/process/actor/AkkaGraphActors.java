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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.tinkerpop.gremlin.process.actor.ActorProgram;
import org.apache.tinkerpop.gremlin.process.actor.ActorsResult;
import org.apache.tinkerpop.gremlin.process.actor.Address;
import org.apache.tinkerpop.gremlin.process.actor.GraphActors;
import org.apache.tinkerpop.gremlin.process.actor.util.DefaultActorsResult;
import org.apache.tinkerpop.gremlin.structure.Partitioner;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AkkaGraphActors<R> implements GraphActors<R> {

    private ActorProgram<R> actorProgram;
    private Partitioner partitioner;
    private boolean executed = false;

    public AkkaGraphActors() {

    }

    @Override
    public String toString() {
        return StringFactory.graphActorsString(this);
    }

    @Override
    public GraphActors<R> program(final ActorProgram<R> actorProgram) {
        this.actorProgram = actorProgram;
        return this;
    }

    @Override
    public GraphActors<R> partitioner(final Partitioner partitioner) {
        this.partitioner = partitioner;
        return this;
    }

    @Override
    public Future<R> submit() {
        if (this.executed)
            throw new IllegalStateException("Can not execute twice");
        this.executed = true;
        final ActorSystem system;
        final ActorsResult<R> result = new DefaultActorsResult<>();

        final Config config = ConfigFactory.defaultApplication().
                withValue("message-priorities",
                        ConfigValueFactory.fromAnyRef(actorProgram.getMessagePriorities().get().stream().map(Class::getCanonicalName).collect(Collectors.toList()).toString()));
        system = ActorSystem.create("traversal-" + UUID.randomUUID(), config);
        try {
            new Address.Master(system.actorOf(Props.create(MasterActor.class, actorProgram, partitioner, result), "master").path().toString(), InetAddress.getLocalHost());
        } catch (final UnknownHostException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return CompletableFuture.supplyAsync(() -> {
            while (!system.isTerminated()) {

            }
            return result.getResult();
        });
    }
}

