/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import javax.script.Bindings;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * The {@link GraphManager} interface allows for reference tracking of Graph references through
 * a {@code Map<String, Graph>}; the interface plugs into the lifeline of gremlin script
 * executions, meaning that commit() and rollback() will be called on all graphs stored in the
 * graph reference tracker at the end of the script executions; you may want to implement
 * this interface if you want to define a custom graph instantiation/closing mechanism; note that
 * the interface also defines similar features for {@link TraversalSource} objects.
 */
public interface GraphManager {

    /**
     * Get a {@link Set} of {@link String} graphNames corresponding to names stored in the graph's
     * reference tracker.
     */
    public Set<String> getGraphNames();

    /**
     * Get {@link Graph} instance whose name matches {@code graphName}.
     *
     * @return {@link Graph} if exists, else null
     */
    public Graph getGraph(final String graphName);

    /**
     * Add or update the specified {@link Graph} with the specified name to {@code Map<String, Graph>} .
     */
    public void putGraph(final String graphName, final Graph g);

    /**
     * Get a {@code Set} of the names of the the stored {@link TraversalSource} instances.
     */
    public Set<String> getTraversalSourceNames();

    /**
     * Get {@link TraversalSource} instance whose name matches {@code traversalSourceName}
     *
     * @return {@link TraversalSource} if exists, else null
     */
    public TraversalSource getTraversalSource(final String traversalSourceName);

    /**
     * Add or update the specified {@link TraversalSource} with the specified name.
     */
    public void putTraversalSource(final String tsName, final TraversalSource ts);

    /**
     * Remove {@link TraversalSource} by name.
     */
    public TraversalSource removeTraversalSource(final String tsName);

    /**
     * Get the {@link Graph} and {@link TraversalSource} list as a set of bindings.
     */
    public Bindings getAsBindings();

    /**
     * Rollback transactions across all {@link Graph} objects.
     */
    public void rollbackAll();

    /**
     * Selectively rollback transactions on the specified graphs or the graphs of traversal sources.
     */
    public void rollback(final Set<String> graphSourceNamesToCloseTxOn);

    /**
     * Commit transactions across all {@link Graph} objects.
     */
    public void commitAll();

    /**
     * Selectively commit transactions on the specified graphs or the graphs of traversal sources.
     */
    public void commit(final Set<String> graphSourceNamesToCloseTxOn);

    /**
     * Implementation that allows for custom graph-opening implementations; if the {@code Map}
     * tracking graph references has a {@link Graph} object corresponding to the graph name, then we return that
     * {@link Graph}-- otherwise, we use the custom {@code Function} to instantiate a new {@link Graph}, add it to
     * the {@link Map} tracking graph references, and return said {@link Graph}.
     */
    public Graph openGraph(final String graphName, final Function<String, Graph> supplier);

    /**
     * Implementation that allows for custom graph closing implementations; this method should remove the {@link Graph}
     * from the {@code GraphManager}.
     */
    public Graph removeGraph(final String graphName) throws Exception;
}
