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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphGraphSONSerializer;

/**
 * The set of serializers that handle the core graph interfaces.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONModule extends SimpleModule {

    public GraphSONModule(final boolean normalize) {
        super("graphson");
        addSerializer(Edge.class, new GraphSONSerializers.EdgeJacksonSerializer(normalize));
        addSerializer(Vertex.class, new GraphSONSerializers.VertexJacksonSerializer(normalize));
        addSerializer(VertexProperty.class, new GraphSONSerializers.VertexPropertyJacksonSerializer(normalize));
        addSerializer(Property.class, new GraphSONSerializers.PropertyJacksonSerializer());
        addSerializer(TraversalMetrics.class, new GraphSONSerializers.TraversalMetricsJacksonSerializer());
        addSerializer(Path.class, new GraphSONSerializers.PathJacksonSerializer());
        addSerializer(StarGraphGraphSONSerializer.DirectionalStarGraph.class, new StarGraphGraphSONSerializer(normalize));
    }
}
