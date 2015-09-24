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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.Serializable;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalClassFunction<S, E> implements Function<Graph, Traversal.Admin<S, E>>, Serializable {

    private final Class<? extends Supplier<Traversal.Admin<S, E>>> traversalSupplierClass;

    public TraversalClassFunction(final Class<? extends Supplier<Traversal.Admin<S, E>>> traversalSupplierClass) {
        this.traversalSupplierClass = traversalSupplierClass;
    }

    public Traversal.Admin<S, E> apply(final Graph graph) {
        try {
            final Traversal.Admin<S, E> traversal = this.traversalSupplierClass.getConstructor().newInstance().get();
            if (!traversal.isLocked()) {
                traversal.setGraph(graph);
                traversal.applyStrategies();
            }
            return traversal;
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public String toString() {
        return this.traversalSupplierClass.getCanonicalName();
    }
}
