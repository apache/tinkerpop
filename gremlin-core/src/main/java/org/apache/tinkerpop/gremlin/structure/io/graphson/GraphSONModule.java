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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphGraphSONSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;

import java.util.Map;

/**
 * The set of serializers that handle the core graph interfaces.  These serializers support normalization which
 * ensures that generated GraphSON will be compatible with line-based versioning tools. This setting comes with
 * some overhead, with respect to key sorting and other in-memory operations.
 * <p/>
 * This is a base class for grouping these core serializers.  Concrete extensions of this class represent a "version"
 * that should be registered with the {@link GraphSONVersion} enum.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
abstract class GraphSONModule extends SimpleModule {

    GraphSONModule(final String name) {
        super(name);
    }

    /**
     * Version 1.0 of GraphSON.
     */
    static final class GraphSONModuleV1d0 extends GraphSONModule {

        /**
         * Constructs a new object.
         */
        protected GraphSONModuleV1d0(final boolean normalize) {
            super("graphson-1.0");
            addSerializer(Edge.class, new GraphSONSerializers.EdgeJacksonSerializer(normalize));
            addSerializer(Vertex.class, new GraphSONSerializers.VertexJacksonSerializer(normalize));
            addSerializer(VertexProperty.class, new GraphSONSerializers.VertexPropertyJacksonSerializer(normalize));
            addSerializer(Property.class, new GraphSONSerializers.PropertyJacksonSerializer());
            addSerializer(TraversalMetrics.class, new GraphSONSerializers.TraversalMetricsJacksonSerializer());
            addSerializer(Path.class, new GraphSONSerializers.PathJacksonSerializer());
            addSerializer(StarGraphGraphSONSerializer.DirectionalStarGraph.class, new StarGraphGraphSONSerializer(normalize));
            addSerializer(Map.Entry.class, new GraphSONSerializers.MapEntryJacksonSerializer());
        }

        public static Builder build() {
            return new Builder();
        }

        static final class Builder implements GraphSONModuleBuilder {

            private Builder() {}

            @Override
            public GraphSONModule create(final boolean normalize) {
                return new GraphSONModuleV1d0(normalize);
            }
        }
    }

    /**
     * A "builder" used to create {@link GraphSONModule} instances.  Each "version" should have an associated
     * {@code GraphSONModuleBuilder} so that it can be registered with the {@link GraphSONVersion} enum.
     */
    static interface GraphSONModuleBuilder {

        /**
         * Creates a new {@link GraphSONModule} object.
         *
         * @param normalize when set to true, keys and objects are ordered to ensure that they are the occur in
         *                  the same order.
         */
        GraphSONModule create(final boolean normalize);
    }
}
