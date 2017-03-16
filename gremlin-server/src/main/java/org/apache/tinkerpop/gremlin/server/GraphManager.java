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
import org.apache.tinkerpop.gremlin.structure.Transaction;

import javax.script.Bindings;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * The {@link GraphManager} interface allows for reference tracking of Graph references through
 * a {@link Map<String, Graph>}; the interface plugs into the lifeline of gremlin script
 * executions, meaning that commit() and rollback() will be called on all graphs stored in the
 * graph reference tracker at the end of the script executions; you may want to implement
 * this interface if you want to define a custom graph instantiation/closing mechanism; note that
 * the interface also defines similar features for {@link TraversalSource} objects.
 */
public interface GraphManager {
    /**
     * @Deprecated This returns a {@link Map} that should be immutable. Please refer to
     * getGraphNames() for replacement.
     *
     * Get a list of the {@link Graph} instances and their binding names
     *
     * @return a {@link Map} where the key is the name of the {@link Graph} and the value is the {@link Graph} itself
     */
    @Deprecated
    public Map<String, Graph> getGraphs();

    /**
     * Get a {@link Set} of {@link String} graphNames corresponding to names stored in the graph's
     * reference tracker.
     */
    public Set<String> getGraphNames();
    /**
     * Get {@link Graph} instance whose name matches {@link gName}
     *
     * @return {@link Graph} if exists, else null
     */
    public Graph getGraph(final String gName);

    /**
     * Add or update {@link Graph} g with name {@link String} gName to
     * {@link Map<String, Graph>}
     */
    public void putGraph(final String gName, final Graph g);

    /**
     * @Deprecated Please treat as immutable {@link Map} and refer to getTraversalSourceNames()
     *
     * Get a list of the {@link TraversalSource} instances and their binding names
     *
     * @return a {@link Map} where the key is the name of the {@link TraversalSource} and the value is the
     *         {@link TraversalSource} itself
     */
    @Deprecated
    public Map<String, TraversalSource> getTraversalSources();

    /**
     * Get {@link TraversalSource} instance whose name matches {@link tsName}
     *
     * @return {@link TraversalSource} if exists, else null
     */

    /**
     * Get a {@link Set} of {@link String} traversalSourceNames to names stored in the
     * traversalSources's reference tracker.
     */
    public Set<String> getTraversalSourceNames();

    public TraversalSource getTraversalSource(final String tsName);
    /**
     * Get the {@link Graph} and {@link TraversalSource} list as a set of bindings.
     */

    /**
     * Add or update {@link TraversalSource} ts with name {@link String} tsName to
     * {@link Map<String, TraversalSource>} returned by call to getTraversalSources()
     */
    public void putTraversalSource(final String tsName, final TraversalSource ts);

    /**
     * Remove {@link TraversalSource} with tsName from {@link Map<String, TraversalSource}.
     */
    public TraversalSource removeTraversalSource(final String tsName);

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
     * Implementation that allows for custom graph-opening implementations; if the {@link Map}
     * tracking graph references has a {@link Graph} object corresponding to the {@link String} graphName,
     * then we return that {@link Graph}-- otherwise, we use the custom {@link Supplier} to instantiate a
     * a new {@link Graph}, add it to the {@link Map} tracking graph references, and return said {@link Graph}.
     */
    public Graph openGraph(final String graphName, final Function<String, Graph> supplier);

    /**
     * Implementation that allows for custom graph-closing implementations;
     * this method should remove the {@link Graph} graph from the {@link Object}
     * tracking {@link Graph} references.
     */
    public Graph removeGraph(final String graphName) throws Exception;
}
