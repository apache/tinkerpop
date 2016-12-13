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
import org.apache.tinkerpop.gremlin.process.actor.Actors;
import org.apache.tinkerpop.gremlin.process.actor.Address;
import org.apache.tinkerpop.gremlin.structure.Partitioner;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AkkaActors<R> implements Actors<R> {

    private final ActorProgram<R> actorProgram;
    private final ActorSystem system;
    private final Address.Master master;

    public AkkaActors(final ActorProgram<R> actorProgram, final Partitioner partitioner) {
        this.actorProgram = actorProgram;
        final Config config = ConfigFactory.defaultApplication().
                withValue("message-priorities",
                        ConfigValueFactory.fromAnyRef(this.actorProgram.getMessagePriorities().stream().map(Class::getCanonicalName).collect(Collectors.toList()).toString()));
        this.system = ActorSystem.create("traversal-" + actorProgram.hashCode(), config);
        this.master = new Address.Master(this.system.actorOf(Props.create(MasterActor.class, this.actorProgram, partitioner), "master").path().toString());
    }

    @Override
    public Address.Master master() {
        return this.master;
    }

    @Override
    public Future<R> submit() {
        return CompletableFuture.supplyAsync(() -> {
            while (!this.system.isTerminated()) {

            }
            return this.actorProgram.getResult();
        });
    }
}

