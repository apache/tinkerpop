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
package org.apache.tinkerpop.gremlin.process.traversal.engine;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
 */
@Deprecated
public final class StandardTraversalEngine implements TraversalEngine {

    private static final StandardTraversalEngine INSTANCE = new StandardTraversalEngine();

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    private StandardTraversalEngine() {

    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    @Override
    public Type getType() {
        return Type.STANDARD;
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    @Override
    public Optional<GraphComputer> getGraphComputer() {
        return Optional.empty();
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    public static Builder build() {
        return Builder.INSTANCE;
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    public static StandardTraversalEngine instance() {
        return INSTANCE;
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    @Override
    public String toString() {
        return this.getClass().getSimpleName().toLowerCase();
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    public final static class Builder implements TraversalEngine.Builder {

        private static final Builder INSTANCE = new Builder();

        /**
         * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
         */
        @Deprecated
        @Override
        public TraversalEngine create(final Graph graph) {
            return StandardTraversalEngine.INSTANCE;
        }
    }
}