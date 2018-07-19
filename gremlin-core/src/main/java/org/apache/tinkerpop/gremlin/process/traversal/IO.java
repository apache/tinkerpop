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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;

/**
 * Fields that can be provided to the {@link GraphTraversalSource#io(String)} using the
 * {@link GraphTraversal#with(String,Object)} step modulator to provide additional configurations.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IO {

    private IO() {}

    /**
     * A value to supply to {@link IO#reader} or {@link IO#writer} to indicate the format to use. Using this shorthand
     * will configure a default {@link GraphSONReader} or {@link GraphSONWriter} respectively,
     */
    public static final String graphson = "graphson";

    /**
     * A value to supply to {@link IO#reader} or {@link IO#writer} to indicate the format to use. Using this shorthand
     * will configure a default {@link GryoReader} or {@link GryoWriter} respectively,
     */
    public static final String gryo = "gryo";

    /**
     * A value to supply to {@link IO#reader} or {@link IO#writer} to indicate the format to use. Using this shorthand
     * will configure a default {@link GraphMLReader} or {@link GraphMLWriter} respectively,
     */
    public static final String graphml = "graphml";

    /**
     * The specific {@link GraphReader} instance to use, the name of the fully qualified classname of such an
     * instance or one of {@link IO#graphson}, {@link IO#gryo} or {@link IO#graphml}. If this value is not specified
     * then {@link GraphTraversalSource#io(String)} will attempt to construct a default {@link GraphReader} based on
     * the file extension provided to it.
     */
    public static final String reader = Graph.Hidden.hide("tinkerpop.io.reader");

    /**
     * The specific {@link GraphWriter} instance to use, the name of the fully qualified classname of such an
     * instance or one of {@link IO#graphson}, {@link IO#gryo} or {@link IO#graphml}. If this value is not specified
     * then {@link GraphTraversalSource#io(String)} will attempt to construct a default {@link GraphWriter} based on
     * the file extension provided to it.
     */
    public static final String writer = Graph.Hidden.hide("tinkerpop.io.writer");

    /**
     * A key that identifies the fully qualified class names of {@link IoRegistry} instances to use. May be specified
     * multiple times (i.e. once for each registry) using the {@link GraphTraversal#with(String, Object)} modulator.
     */
    public static final String registry = Graph.Hidden.hide("tinkerpop.io.registry");
}
