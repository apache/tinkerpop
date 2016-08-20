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

public interface GraphManager {
    /**
     * Get a list of the {@link Graph} instances and their binding names
     *
     * @return a {@link Map} where the key is the name of the {@link Graph} and the value is the {@link Graph} itself
     */
    public Map<String, Graph> getGraphs();

    /**
     * Get a list of the {@link TraversalSource} instances and their binding names
     *
     * @return a {@link Map} where the key is the name of the {@link TraversalSource} and the value is the
     *         {@link TraversalSource} itself
     */
    public Map<String, TraversalSource> getTraversalSources();

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
}
