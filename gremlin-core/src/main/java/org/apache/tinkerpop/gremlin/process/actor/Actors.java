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
import org.apache.tinkerpop.gremlin.process.actor.traversal.strategy.decoration.ActorProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Partitioner;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Actors implements Processor.Description<GraphActors> {

    private final Class<? extends GraphActors> graphActorsClass;
    private Partitioner partitioner = null;

    private Actors(final Class<? extends GraphActors> graphActorsClass) {
        this.graphActorsClass = graphActorsClass;
    }

    public static Actors of(final Class<? extends GraphActors> graphActorsClass) {
        return new Actors(graphActorsClass);
    }

    public Actors partitioner(final Partitioner partitioner) {
        final Actors clone = this.clone();
        clone.partitioner = partitioner;
        return clone;
    }

    public Class<? extends GraphActors> getGraphActorsClass() {
        return this.graphActorsClass;
    }

    public Partitioner getPartitioner() {
        return this.partitioner;
    }


    @Override
    public String toString() {
        return this.graphActorsClass.getSimpleName().toLowerCase();
    }

    public Actors clone() {
        try {
            return (Actors) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void addTraversalStrategies(final TraversalSource traversalSource) {
        final ActorProgramStrategy actorProgramStrategy = new ActorProgramStrategy(this);
        traversalSource.getStrategies().addStrategies(actorProgramStrategy);
    }
}
