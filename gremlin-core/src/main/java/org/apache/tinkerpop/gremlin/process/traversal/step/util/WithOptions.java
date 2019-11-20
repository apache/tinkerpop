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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IndexStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * Configuration options to be passed to the {@link GraphTraversal#with(String, Object)}.
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class WithOptions {

    //
    // PropertyMapStep
    //

    /**
     * Configures the tokens to be included in value maps.
     */
    public static final String tokens = Graph.Hidden.hide("tinkerpop.valueMap.tokens");

    /**
     * Include no tokens.
     */
    public static int none = 0;

    /**
     * Include ids (affects all {@link Element} value maps).
     */
    public static int ids = 1;

    /**
     * Include labels (affects all {@link Vertex} and {@link Edge} value maps).
     */
    public static int labels = 2;

    /**
     * Include keys (affects all {@link VertexProperty} value maps).
     */
    public static int keys = 4;

    /**
     * Include keys (affects all {@link VertexProperty} value maps).
     */
    public static int values = 8;

    /**
     * Include all tokens.
     */
    public static int all = ids | labels | keys | values;

    //
    // IndexStep
    //

    /**
     * Configures the indexer to be used in {@link IndexStep}.
     */
    public static final String indexer = Graph.Hidden.hide("tinkerpop.index.indexer");

    /**
     * Index items using 2-item lists.
     */
    public static int list = 0;

    /**
     * Index items using a {@code LinkedHashMap}.
     */
    public static int map = 1;
}
