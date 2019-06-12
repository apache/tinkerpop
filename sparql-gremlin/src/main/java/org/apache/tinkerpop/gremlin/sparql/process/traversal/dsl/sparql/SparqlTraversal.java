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
package org.apache.tinkerpop.gremlin.sparql.process.traversal.dsl.sparql;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

/**
 * The {@code SparqlTraversal} has no additional traversal steps. The only step available for "SPARQL" is the
 * {@link SparqlTraversalSource#sparql(String)} start step.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface SparqlTraversal<S, E> extends Traversal<S, E> {
    public interface Admin<S, E> extends Traversal.Admin<S, E>, SparqlTraversal<S, E> {

        @Override
        public default <E2> SparqlTraversal.Admin<S, E2> addStep(final Step<?, E2> step) {
            return (SparqlTraversal.Admin<S, E2>) Traversal.Admin.super.addStep((Step) step);
        }

        @Override
        public default SparqlTraversal<S, E> iterate() {
            return SparqlTraversal.super.iterate();
        }

        @Override
        public SparqlTraversal.Admin<S, E> clone();
    }

    @Override
    public default SparqlTraversal.Admin<S, E> asAdmin() {
        return (SparqlTraversal.Admin<S, E>) this;
    }

    ////

    /**
     * Iterates the traversal presumably for the generation of side-effects.
     */
    @Override
    public default SparqlTraversal<S, E> iterate() {
        Traversal.super.iterate();
        return this;
    }
}
