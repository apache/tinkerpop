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

import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultSparqlTraversal<S, E> extends DefaultTraversal<S, E> implements SparqlTraversal.Admin<S, E> {

    public DefaultSparqlTraversal() {
        super();
    }

    public DefaultSparqlTraversal(final SparqlTraversalSource sparqlTraversalSource) {
        super(sparqlTraversalSource);
    }

    public DefaultSparqlTraversal(final Graph graph) {
        super(graph);
    }

    @Override
    public SparqlTraversal.Admin<S, E> asAdmin() {
        return this;
    }

    @Override
    public SparqlTraversal<S, E> iterate() {
        return SparqlTraversal.Admin.super.iterate();
    }

    @Override
    public DefaultSparqlTraversal<S, E> clone() {
        return (DefaultSparqlTraversal<S, E>) super.clone();
    }
}
