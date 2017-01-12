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
import akka.dispatch.RequiresMessageQueue;
import akka.japi.pf.ReceiveBuilder;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.actors.Actor;
import org.apache.tinkerpop.gremlin.process.actors.ActorProgram;
import org.apache.tinkerpop.gremlin.process.actors.Address;
import org.apache.tinkerpop.gremlin.process.actors.GraphActors;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.Partitioner;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.partitioner.HashPartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WorkerActor extends AbstractActor implements RequiresMessageQueue<ActorMailbox.ActorSemantics>, Actor.Worker {

    private final ActorProgram.Worker workerProgram;
    private final Partition localPartition;
    private final Partitioner partitioner;
    private final Address.Worker self;
    private final Address.Master master;
    private final List<Address.Worker> workers;
    private final Map<Address, ActorSelection> actors = new HashMap<>();

    public WorkerActor(final Configuration configuration, final String partitionId, final Address.Master master) {
        final Graph graph = GraphFactory.open(configuration);
        final ActorProgram actorProgram = ActorProgram.createActorProgram(graph, configuration);
        final int workers = configuration.getInt(GraphActors.GRAPH_ACTORS_WORKERS, 1);
        this.partitioner = workers == 1 ? graph.partitioner() : new HashPartitioner(graph.partitioner(), workers);
        this.localPartition = this.partitioner.getPartition(partitionId);
        this.self = new Address.Worker(this.createWorkerAddress(this.localPartition), this.localPartition.location());
        this.master = master;
        this.workers = new ArrayList<>();
        for (final Partition partition : this.partitioner.getPartitions()) {
            this.workers.add(new Address.Worker(this.createWorkerAddress(partition), partition.location()));
        }
        this.workerProgram = actorProgram.createWorkerProgram(this);
        receive(ReceiveBuilder.matchAny(this.workerProgram::execute).build());
    }

    @Override
    public void preStart() {
        this.workerProgram.setup();
    }

    @Override
    public void postStop() {
        this.workerProgram.terminate();
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
    public Partition partition() {
        return this.localPartition;
    }

    @Override
    public Partitioner partitioner() {
        return this.partitioner;
    }

    @Override
    public Address.Worker address() {
        return this.self;
    }

    @Override
    public Address.Master master() {
        return this.master;
    }

    private String createWorkerAddress(final Partition partition) {
        return "../worker-" + partition.id();
    }
}

