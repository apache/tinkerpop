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

package org.apache.tinkerpop.gremlin.process;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;

import java.io.Serializable;

/**
 * This is a marker interface that denotes that the respective implementation is able to evaluate/execute/process a
 * {@link org.apache.tinkerpop.gremlin.process.traversal.Traversal}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Processor {

    /**
     * A {@link Processor} description provides the necessary configuration to create a {@link Processor}.
     * This also entails {@link org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy} creation
     * for a {@link TraversalSource}.
     *
     * @param <P> The type of {@link Processor} this description is used for.
     */
    public static interface Description<P extends Processor> extends Cloneable, Serializable {

        /**
         * Add respective {@link org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies} to the
         * provided {@link TraversalSource}.
         *
         * @param traversalSource the traversal source to add processor-specific strategies to
         */
        public TraversalSource addTraversalStrategies(final TraversalSource traversalSource);
    }

}
