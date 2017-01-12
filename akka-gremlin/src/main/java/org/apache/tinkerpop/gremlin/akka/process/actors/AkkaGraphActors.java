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

import akka.actor.ActorSystem;
import akka.actor.Deploy;
import akka.actor.Props;
import akka.remote.RemoteScope;
import com.typesafe.config.Config;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.process.actors.ActorProgram;
import org.apache.tinkerpop.gremlin.process.actors.ActorsResult;
import org.apache.tinkerpop.gremlin.process.actors.GraphActors;
import org.apache.tinkerpop.gremlin.process.actors.util.DefaultActorsResult;
import org.apache.tinkerpop.gremlin.process.actors.util.GraphActorsHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.config.SerializableConfiguration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AkkaGraphActors<R> implements GraphActors<R> {

    private ActorProgram actorProgram;
    private Configuration configuration;
    private boolean executed = false;

    private AkkaGraphActors(final Configuration configuration) {
        this.configuration = new SerializableConfiguration(configuration);
        this.configuration.setProperty(GRAPH_ACTORS, AkkaGraphActors.class.getCanonicalName());
        GraphActorsHelper.configure(this, this.configuration);
    }

    @Override
    public String toString() {
        return StringFactory.graphActorsString(this);
    }

    @Override
    public GraphActors<R> program(final ActorProgram actorProgram) {
        this.actorProgram = actorProgram;
        actorProgram.storeState(this.configuration);
        return this;
    }

    @Override
    public GraphActors<R> workers(final int workers) {
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
        ///////
        final Configuration finalConfiguration = new SerializableConfiguration(graph.configuration());
        ConfigurationUtils.copy(this.configuration, finalConfiguration);
        final Config config = AkkaConfigFactory.generateAkkaConfig(this.actorProgram, finalConfiguration);
        final ActorSystem system = ActorSystem.create("tinkerpop", config);
        final ActorsResult<R> result = new DefaultActorsResult<>();
        ///////
        final akka.actor.Address masterAddress = AkkaConfigFactory.getMasterActorDeployment(config);
        system.actorOf(Props.create(MasterActor.class, finalConfiguration, result).withDeploy(new Deploy(new RemoteScope(masterAddress))), "master");

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

