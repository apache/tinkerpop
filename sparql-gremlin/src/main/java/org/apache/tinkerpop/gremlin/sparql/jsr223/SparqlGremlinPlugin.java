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
package org.apache.tinkerpop.gremlin.sparql.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.sparql.process.traversal.dsl.sparql.DefaultSparqlTraversal;
import org.apache.tinkerpop.gremlin.sparql.process.traversal.dsl.sparql.SparqlTraversal;
import org.apache.tinkerpop.gremlin.sparql.process.traversal.dsl.sparql.SparqlTraversalSource;
import org.apache.tinkerpop.gremlin.sparql.process.traversal.strategy.SparqlStrategy;

/**
 * {@link GremlinPlugin} implementation for {@code sparql-gremlin} that imports the key classes and interfaces required
 * to use SPARQL in TinkerPop.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class SparqlGremlinPlugin extends AbstractGremlinPlugin {

    private static final String NAME = "tinkerpop.sparql";

    private static final ImportCustomizer imports;

    static {
        try {
            imports = DefaultImportCustomizer.build().addClassImports(
                    SparqlTraversalSource.class,
                    SparqlTraversal.class,
                    DefaultSparqlTraversal.class,
                    SparqlStrategy.class).create();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static final SparqlGremlinPlugin instance = new SparqlGremlinPlugin();

    public SparqlGremlinPlugin() {
        super(NAME, imports);
    }

    public static SparqlGremlinPlugin instance() {
        return instance;
    }
}
