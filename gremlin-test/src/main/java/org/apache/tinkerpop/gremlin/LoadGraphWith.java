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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.*;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES;

/**
 * Annotations to define a graph example to load from test resources prior to test execution.  This annotation is
 * for use only with test that extend from {@link AbstractGremlinTest}.
 * <p/>
 * Note that the annotation assumes that the @{link GraphData} referenced by the annotation can be made available
 * to the graph in the test.  In other words, even a graph that has "read-only" features, should have some method of
 * getting the data available to it.  That said, graphs must minimally support the data types that the sample
 * data contains for the test to be executed.
 * <p/>
 * If a graph implementation is "read-only", it can override the
 * {@link GraphProvider#loadGraphData(org.apache.tinkerpop.gremlin.structure.Graph, LoadGraphWith, Class, String)} method
 * to provide some other mechanism for making that data available to the graph in time for the test.  See the
 * hadoop-gremlin implementation for more details.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface LoadGraphWith {

    public enum GraphData {
        /**
         * Loads the "classic" TinkerPop toy graph.
         */
        CLASSIC,

        /**
         * Classic in TinkerPop2 legacy format.
         */
        CLASSIC_TP2,

        /**
         * Classic in TinkerPop2 adjacency list format.
         */
        CLASSIC_TP2_ADJ,

        /**
         * Loads the "modern" TinkerPop toy graph which is like "classic", but with the "weight" value on edges stored
         * as double and labels added for vertices.  This should be the most commonly used graph instance for testing
         * as graphs that support string, double and int should comprise the largest number of implementations.
         */
        MODERN,

        /**
         * Modern in TinkerPop2 legacy format.
         */
        MODERN_TP2,

        /**
         * Modern in TinkerPop2 adjacency list format.
         */
        MODERN_TP2_ADJ,

        /**
         * Load "The Crew" TinkerPop3 toy graph which includes {@link org.apache.tinkerpop.gremlin.structure.VertexProperty} data.
         */
        CREW,

        /**
         * Crew in TinkerPop2 legacy format.
         */
        CREW_TP2,

        /**
         * Crew in TinkerPop2 adjacency list format.
         */
        CREW_TP2_ADJ,

        /**
         * Loads the "grateful dead" graph which is a "large" graph which provides for the construction of more
         * complex traversals.
         */
        GRATEFUL,

        /**
         * Grateful in TinkerPop2 legacy format.
         */
        GRATEFUL_TP2,

        /**
         * Grateful in TinkerPop2 adjacency list format.
         */
        GRATEFUL_TP2_ADJ;

        private static final List<FeatureRequirement> featuresRequiredByClassic = new ArrayList<FeatureRequirement>() {{
            add(FeatureRequirement.Factory.create(FEATURE_STRING_VALUES, VertexPropertyFeatures.class));
            add(FeatureRequirement.Factory.create(FEATURE_INTEGER_VALUES, VertexPropertyFeatures.class));
            add(FeatureRequirement.Factory.create(FEATURE_FLOAT_VALUES, EdgePropertyFeatures.class));
        }};

        private static final List<FeatureRequirement> featuresRequiredByCrew = new ArrayList<FeatureRequirement>() {{
            add(FeatureRequirement.Factory.create(FEATURE_STRING_VALUES, VertexPropertyFeatures.class));
            add(FeatureRequirement.Factory.create(FEATURE_INTEGER_VALUES, VertexPropertyFeatures.class));
            add(FeatureRequirement.Factory.create(FEATURE_BOOLEAN_VALUES, VertexPropertyFeatures.class));
            add(FeatureRequirement.Factory.create(FEATURE_META_PROPERTIES, Graph.Features.VertexFeatures.class));
            add(FeatureRequirement.Factory.create(FEATURE_MULTI_PROPERTIES, Graph.Features.VertexFeatures.class));
        }};

        private static final List<FeatureRequirement> featuresRequiredByModern = new ArrayList<FeatureRequirement>() {{
            add(FeatureRequirement.Factory.create(FEATURE_STRING_VALUES, VertexPropertyFeatures.class));
            add(FeatureRequirement.Factory.create(FEATURE_INTEGER_VALUES, VertexPropertyFeatures.class));
            add(FeatureRequirement.Factory.create(FEATURE_DOUBLE_VALUES, EdgePropertyFeatures.class));
        }};

        private static final List<FeatureRequirement> featuresRequiredByGrateful = new ArrayList<FeatureRequirement>() {{
            add(FeatureRequirement.Factory.create(FEATURE_STRING_VALUES, VertexPropertyFeatures.class));
            add(FeatureRequirement.Factory.create(FEATURE_INTEGER_VALUES, VertexPropertyFeatures.class));
        }};

        public String location() {
            switch (this) {
                case CLASSIC:
                    return RESOURCE_PATH_PREFIX + "tinkerpop-classic.kryo";
                case CREW:
                    return RESOURCE_PATH_PREFIX + "tinkerpop-crew.kryo";
                case MODERN:
                    return RESOURCE_PATH_PREFIX + "tinkerpop-modern.kryo";
                case GRATEFUL:
                    return RESOURCE_PATH_PREFIX + "grateful-dead.kryo";
            }

            throw new RuntimeException("No file for this GraphData type");
        }

        public List<FeatureRequirement> featuresRequired() {
            switch (this) {
                case CLASSIC: case CLASSIC_TP2: CLASSIC_TP2_ADJ:
                    return featuresRequiredByClassic;
                case CREW: case CREW_TP2: case CREW_TP2_ADJ:
                    return featuresRequiredByCrew;
                case MODERN: case MODERN_TP2: case MODERN_TP2_ADJ:
                    return featuresRequiredByModern;
                case GRATEFUL: case GRATEFUL_TP2: case GRATEFUL_TP2_ADJ:
                    return featuresRequiredByGrateful;
            }
            throw new RuntimeException("No features for this GraphData type");
        }
    }

    public static final String RESOURCE_PATH_PREFIX = "/org/apache/tinkerpop/gremlin/structure/io/gryo/";

    /**
     * The name of the resource to load with full path.
     */
    public GraphData value() default GraphData.CLASSIC;

}
