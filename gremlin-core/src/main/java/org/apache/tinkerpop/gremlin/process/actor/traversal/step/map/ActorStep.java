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

package org.apache.tinkerpop.gremlin.process.actor.traversal.step.map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

import org.apache.tinkerpop.gremlin.process.actor.Actors;
import org.apache.tinkerpop.gremlin.process.actor.traversal.strategy.decoration.ActorStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.structure.Partitioner;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ActorStep<S, E> extends AbstractStep<E, E> {

    private final Class<? extends Actors> actorsClass;
    private final Traversal.Admin<S, E> partitionTraversal;
    private final Partitioner partitioner;

    private boolean first = true;


    public ActorStep(final Traversal.Admin<?, ?> traversal, final Class<? extends Actors> actorsClass, final Partitioner partitioner) {
        super(traversal);
        this.actorsClass = actorsClass;
        this.partitionTraversal = (Traversal.Admin) traversal.clone();
        final TraversalStrategies strategies = this.partitionTraversal.getStrategies().clone();
        strategies.removeStrategies(ActorStrategy.class);
        strategies.addStrategies(VertexProgramStrategy.instance());
        this.partitionTraversal.setStrategies(strategies);
        this.partitioner = partitioner;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.partitionTraversal);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (this.first) {
            this.first = false;
            try {
                final Actors<S, E> actors = this.actorsClass.getConstructor(Traversal.Admin.class, Partitioner.class).newInstance(this.partitionTraversal, this.partitioner);
                actors.submit().get().forEach(this.starts::add);
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
        return this.starts.next();
    }
}

