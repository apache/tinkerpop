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

import akka.actor.AbstractActor;
import akka.actor.ActorSelection;
import akka.actor.Deploy;
import akka.actor.Props;
import akka.dispatch.RequiresMessageQueue;
import akka.japi.pf.ReceiveBuilder;
import akka.remote.RemoteScope;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.actors.Actor;
import org.apache.tinkerpop.gremlin.process.actors.ActorProgram;
import org.apache.tinkerpop.gremlin.process.actors.ActorsResult;
import org.apache.tinkerpop.gremlin.process.actors.Address;
import org.apache.tinkerpop.gremlin.process.actors.GraphActors;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.Partitioner;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.partitioner.HashPartitioner;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MasterActor extends AbstractActor implements RequiresMessageQueue<ActorMailbox.ActorSemantics>, Actor.Master {

    private final ActorProgram.Master masterProgram;
    private final Address.Master master;
    private final List<Address.Worker> workers;
    private final Map<Address, ActorSelection> actors = new HashMap<>();
    private final ActorsResult<?> result;
    private final Partitioner partitioner;

    public MasterActor(final Configuration configuration, final ActorsResult<?> result) {
        final Graph graph = GraphFactory.open(configuration);
        final ActorProgram actorProgram = ActorProgram.createActorProgram(graph, configuration);
        final int workers = configuration.getInt(GraphActors.GRAPH_ACTORS_WORKERS, 1);
        this.partitioner = workers == 1 ? graph.partitioner() : new HashPartitioner(graph.partitioner(), workers);
        this.result = result;
        try {
            this.master = new Address.Master(self().path().toString(), InetAddress.getLocalHost());
        } catch (final UnknownHostException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        this.workers = new ArrayList<>();
        final List<Partition> partitions = partitioner.getPartitions();
        for (final Partition partition : partitions) {
            final Address.Worker workerAddress = new Address.Worker("worker-" + partition.id(), partition.location());
            this.workers.add(workerAddress);
            context().actorOf(
                    Props.create(WorkerActor.class, configuration, partition.id(), this.master)
                            .withDeploy(new Deploy(new RemoteScope(AkkaConfigFactory.getWorkerActorDeployment(configuration, partition)))),
                    workerAddress.getId());
        }
        this.masterProgram = actorProgram.createMasterProgram(this);
        receive(ReceiveBuilder.matchAny(this.masterProgram::execute).build());
    }

    @Override
    public void preStart() {
        this.masterProgram.setup();
    }

    @Override
    public void postStop() {
        this.masterProgram.terminate();
    }

    @Override
    public <M> void send(final Address toActor, final M message) {
        ActorSelection actor = this.actors.get(toActor);
        if (null == actor) {
            actor = context().actorSelection(toActor.getId());
            this.actors.put(toActor, actor);
        }
        actor.tell(message, self());
    }

    @Override
    public List<Address.Worker> workers() {
        return this.workers;
    }

    @Override
    public Partitioner partitioner() {
        return this.partitioner;
    }

    @Override
    public Address.Master address() {
        return this.master;
    }

    @Override
    public void close() {
        this.context().system().stop(self());
        context().system().terminate();
    }

    @Override
    public <R> ActorsResult<R> result() {
        return (ActorsResult<R>) this.result;
    }

}
