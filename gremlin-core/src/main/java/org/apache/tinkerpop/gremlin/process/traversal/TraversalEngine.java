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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * A TraversalEngine is reponsible for executing a {@link Traversal}. There are two {@link Type}s of TraversalEngines.
 * {@link Type#STANDARD} is the OLTP, iterator-based model of graph traversal.
 * {@link Type#COMPUTER} is the OLAP, message passing model of graph traversal.
 * Every {@link TraversalSource} should be provided a {@link TraversalEngine.Builder} so it can construct an engine for each spawned {@link Traversal}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @deprecated As of release 3.2.0, replaced by {@link Computer}.
 */
@Deprecated
public interface TraversalEngine extends Serializable {

    public enum Type {STANDARD, COMPUTER}

    /**
     * Get the type of the engine: {@link Type#STANDARD} or {@link Type#COMPUTER}.
     *
     * @return the traversal engine type
     */
    public Type getType();

    /**
     * If the traversal engine is of type {@link Type#COMPUTER}, then it should have the {@link GraphComputer} used for executing the traversal.
     *
     * @return an optional of containing the graph computer to be used for execution.
     */
    public Optional<GraphComputer> getGraphComputer();

    /**
     * Whether or not the type is {@link Type#STANDARD}.
     *
     * @return whether the engine type is standard (OLTP).
     */
    public default boolean isStandard() {
        return this.getType().equals(Type.STANDARD);
    }

    /**
     * Whether or not the type is {@link Type#COMPUTER}.
     *
     * @return whether the engine type is computer (OLAP).
     */
    public default boolean isComputer() {
        return this.getType().equals(Type.COMPUTER);
    }

    ///////////

    @Deprecated
    public interface Builder extends Serializable {

        /**
         * A list of {@link TraversalStrategy} instances that should be applied to the ultimate {@link Traversal}.
         *
         * @return strategies to apply (if any).
         */
        public default List<TraversalStrategy> getWithStrategies() {
            return Collections.emptyList();
        }

        /**
         * A list of {@link TraversalStrategy} classes that should not be applied to the ultimate {@link Traversal}.
         *
         * @return strategies to not apply (if any).
         */
        public default List<Class<? extends TraversalStrategy>> getWithoutStrategies() {
            return Collections.emptyList();
        }

        /**
         * Create the {@link TraversalEngine}.
         *
         * @param graph the graph to ultimately have the {@link Traversal} execute over.
         * @return a {@link TraversalEngine} that is particular to a {@link Traversal}.
         */
        public TraversalEngine create(final Graph graph);
    }
}
