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
package org.apache.tinkerpop.gremlin;

import org.apache.tinkerpop.gremlin.process.TraversalEngine;

/**
 * Holds objects specified by the test suites supplying them in a static manner to the test cases.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphManager {
    private static GraphProvider graphProvider;
    private static TraversalEngine.Type traversalEngineType;

    public static GraphProvider setGraphProvider(final GraphProvider graphProvider) {
        final GraphProvider old = GraphManager.graphProvider;
        GraphManager.graphProvider = graphProvider;
        return old;
    }

    public static GraphProvider getGraphProvider() {
        return graphProvider;
    }

    public static TraversalEngine.Type setTraversalEngineType(final TraversalEngine.Type traversalEngine) {
        final TraversalEngine.Type old = GraphManager.traversalEngineType;
        GraphManager.traversalEngineType = traversalEngine;
        return old;
    }

    public static TraversalEngine.Type getTraversalEngineType() {
        return traversalEngineType;
    }
}
