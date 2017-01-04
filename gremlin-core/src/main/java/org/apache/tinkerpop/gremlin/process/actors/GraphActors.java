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

package org.apache.tinkerpop.gremlin.process.actors;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.Processor;
import org.apache.tinkerpop.gremlin.process.actors.traversal.strategy.decoration.ActorProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.ProcessorTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.concurrent.Future;

/**
 * GraphActors is a message-passing based graph {@link Processor} that is:
 * asynchronous, distributed, and partition-centric.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphActors<R> extends Processor {

    public static final String GRAPH_ACTORS = "gremlin.graphActors";
    public static final String GRAPH_ACTORS_WORKERS = "gremlin.graphActors.workers";

    /**
     * Provide the {@link ActorProgram} that the GraphActors will execute.
     *
     * @param program the program to execute
     * @return the updated GraphActors with newly defined program
     */
    public GraphActors<R> program(final ActorProgram program);

    /**
     * Specify the number of workers per {@link Graph} {@link org.apache.tinkerpop.gremlin.structure.Partition}.
     *
     * @param workers the number of workers per partition
     * @return the updated GraphActors with newly defined workers
     */
    public GraphActors<R> workers(final int workers);

    /**
     * Add an arbitrary configuration to the GraphActors system.
     * Typically, these configurations are provider-specific and do not generalize across all GraphActor implementations.
     *
     * @param key   the key of the configuration
     * @param value the value of the configuration
     * @return the updated GraphActors with newly defined configuration
     */
    public GraphActors<R> configure(final String key, final Object value);

    /**
     * Execute the {@link ActorProgram} on the {@link GraphActors} system against the specified {@link Graph}.
     *
     * @return a {@link Future} denoting a reference to the asynchronous computation's result
     */
    @Override
    public Future<R> submit(final Graph graph);

    /**
     * Returns an {@link ActorProgramStrategy} which enables a {@link Traversal} to execute on {@link GraphActors}.
     *
     * @return a {@link org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy} capable of executing traversals on a GraphActors system
     */
    @Override
    public default ProcessorTraversalStrategy<GraphActors> getProcessorTraversalStrategy() {
        return new ActorProgramStrategy(this);
    }

    /**
     * Create an arbitrary GraphActors system given the information contained in the provided {@link Configuration}.
     *
     * @param configuration the {@link Configuration} containing, at minimum, {@link GraphActors#GRAPH_ACTORS} system class name
     * @param <A>           the particular type of GraphActors
     * @return a constructed GraphActors system
     */
    public static <A extends GraphActors> A open(final Configuration configuration) {
        try {
            return (A) Class.forName(configuration.getString(GRAPH_ACTORS)).getMethod("open", Configuration.class).invoke(null, configuration);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    public static <A extends GraphActors> A open(final Class<A> graphActorsClass) {
        final BaseConfiguration configuration = new BaseConfiguration();
        configuration.setProperty(GRAPH_ACTORS, graphActorsClass.getCanonicalName());
        return GraphActors.open(configuration);
    }
}
