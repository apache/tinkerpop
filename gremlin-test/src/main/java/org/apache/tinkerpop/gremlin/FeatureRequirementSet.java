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

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;

/**
 * Logically grouped features used to simplify test annotations.  This annotation can be used in conjunction
 * with {@link FeatureRequirement} and features automatically added by {@link LoadGraphWith}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface FeatureRequirementSet {

    public enum Package {
        /**
         * Allows for the most basic features of a graph - add edges/vertices withs support for string property values.
         */
        SIMPLE,

        /**
         * Allows for adding of vertices (but not edges) with string property values.
         */
        VERTICES_ONLY;

        private static final List<FeatureRequirement> featuresRequiredBySimple = new ArrayList<FeatureRequirement>() {{
            add(FeatureRequirement.Factory.create(Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES, Graph.Features.VertexFeatures.class));
            add(FeatureRequirement.Factory.create(Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES, Graph.Features.EdgeFeatures.class));
            add(FeatureRequirement.Factory.create(Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY, Graph.Features.VertexFeatures.class));
            add(FeatureRequirement.Factory.create(Graph.Features.VertexPropertyFeatures.FEATURE_STRING_VALUES, Graph.Features.VertexPropertyFeatures.class));
            add(FeatureRequirement.Factory.create(Graph.Features.EdgePropertyFeatures.FEATURE_STRING_VALUES, Graph.Features.EdgePropertyFeatures.class));
            add(FeatureRequirement.Factory.create(Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY, Graph.Features.EdgeFeatures.class));
        }};

        private static final List<FeatureRequirement> featuresRequiredByVerticesOnly = new ArrayList<FeatureRequirement>() {{
            add(FeatureRequirement.Factory.create(Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES, Graph.Features.VertexFeatures.class));
            add(FeatureRequirement.Factory.create(Graph.Features.VertexPropertyFeatures.FEATURE_STRING_VALUES, Graph.Features.VertexPropertyFeatures.class));
            add(FeatureRequirement.Factory.create(Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY, Graph.Features.VertexFeatures.class));
        }};

        public List<FeatureRequirement> featuresRequired() {
            switch (this) {
                case SIMPLE:
                    return featuresRequiredBySimple;
                case VERTICES_ONLY:
                    return featuresRequiredByVerticesOnly;
            }

            throw new RuntimeException("No features for this GraphData type");
        }
    }

    /**
     * The set of feature requirements to require for the test.
     */
    public Package value() default Package.SIMPLE;
}
