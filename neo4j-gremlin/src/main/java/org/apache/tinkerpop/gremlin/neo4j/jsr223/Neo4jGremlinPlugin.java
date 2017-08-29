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
package org.apache.tinkerpop.gremlin.neo4j.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.neo4j.process.traversal.LabelP;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jElement;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraphVariables;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jHelper;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jProperty;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertexProperty;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Neo4jGremlinPlugin extends AbstractGremlinPlugin {

    private static final String NAME = "tinkerpop.neo4j";

    private static final ImportCustomizer imports;

    static {
        try {
            imports = DefaultImportCustomizer.build()
                    .addClassImports(Neo4jEdge.class,
                            Neo4jElement.class,
                            Neo4jGraph.class,
                            Neo4jGraphVariables.class,
                            Neo4jHelper.class,
                            Neo4jProperty.class,
                            Neo4jVertex.class,
                            Neo4jVertexProperty.class,
                            LabelP.class)
                    .addMethodImports(LabelP.class.getMethod("of", String.class)).create();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static final Neo4jGremlinPlugin instance = new Neo4jGremlinPlugin();

    public Neo4jGremlinPlugin() {
        super(NAME, imports);
    }

    public static Neo4jGremlinPlugin instance() {
        return instance;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}