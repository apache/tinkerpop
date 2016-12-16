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
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.process.actor.ActorProgram;
import org.apache.tinkerpop.gremlin.process.actor.ActorsResult;
import org.apache.tinkerpop.gremlin.process.actor.Address;
import org.apache.tinkerpop.gremlin.process.actor.GraphActors;
import org.apache.tinkerpop.gremlin.process.actor.util.DefaultActorsResult;
import org.apache.tinkerpop.gremlin.process.actor.util.GraphActorsHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Partitioner;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.partitioner.HashPartitioner;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AkkaGraphActors<R> implements GraphActors<R> {

    private ActorProgram<R> actorProgram;
    private int workers = 1;
    private Configuration configuration;
    private boolean executed = false;

    private AkkaGraphActors(final Configuration configuration) {
        this.configuration = new BaseConfiguration();
        ConfigurationUtils.copy(configuration, this.configuration);
        this.configuration.setProperty(GRAPH_ACTORS, AkkaGraphActors.class.getCanonicalName());
        GraphActorsHelper.configure(this, this.configuration);
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
    public GraphActors<R> workers(final int workers) {
        this.workers = workers;
        this.configuration.setProperty(GRAPH_ACTORS_WORKERS, workers);
        return this;
    }

    @Override
    public GraphActors<R> configure(final String key, final Object value) {
        this.configuration.setProperty(key, value);
        return this;
    }

    @Override
    public Future<R> submit(final Graph graph) {
        if (this.executed)
            throw new IllegalStateException("Can not execute twice");
        this.executed = true;
        final Config config = ConfigFactory.defaultApplication().withValue("message-priorities",
                ConfigValueFactory.fromAnyRef(this.actorProgram.getMessagePriorities().
                        orElse(Collections.singletonList(Object.class)).
                        stream().
                        map(Class::getCanonicalName).
                        collect(Collectors.toList()).toString()));
        final ActorSystem system = ActorSystem.create("traversal-" + UUID.randomUUID(), config);
        final ActorsResult<R> result = new DefaultActorsResult<>();
        final Partitioner partitioner = this.workers == 1 ? graph.partitioner() : new HashPartitioner(graph.partitioner(), this.workers);
        try {
            new Address.Master(system.actorOf(Props.create(MasterActor.class, this.actorProgram, partitioner, result), "master").path().toString(), InetAddress.getLocalHost());
        } catch (final UnknownHostException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return CompletableFuture.supplyAsync(() -> {
            while (!system.isTerminated()) {

            }
            return result.getResult();
        });
    }

    @Override
    public Configuration configuration() {
        return this.configuration;
    }

    public static AkkaGraphActors open(final Configuration configuration) {
        return new AkkaGraphActors(configuration);
    }

    public static AkkaGraphActors open() {
        return new AkkaGraphActors(new BaseConfiguration());
    }
}

