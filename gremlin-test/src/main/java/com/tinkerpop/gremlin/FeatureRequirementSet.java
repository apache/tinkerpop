package com.tinkerpop.gremlin;

import com.tinkerpop.gremlin.structure.Graph;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;

/**
 * Logically grouped features used to simplify test annotations.  This annotation can be used in conjunction
 * with {@link com.tinkerpop.gremlin.FeatureRequirement} and features automatically added by
 * {@link com.tinkerpop.gremlin.LoadGraphWith}.
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
            add(FeatureRequirement.Factory.create(Graph.Features.VertexPropertyFeatures.FEATURE_ADD_PROPERTY, Graph.Features.VertexPropertyFeatures.class));
            add(FeatureRequirement.Factory.create(Graph.Features.VertexPropertyFeatures.FEATURE_STRING_VALUES, Graph.Features.VertexPropertyFeatures.class));
            add(FeatureRequirement.Factory.create(Graph.Features.VertexPropertyFeatures.FEATURE_STRING_VALUES, Graph.Features.EdgePropertyFeatures.class));
            add(FeatureRequirement.Factory.create(Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY, Graph.Features.EdgeFeatures.class));
        }};

        private static final List<FeatureRequirement> featuresRequiredByVerticesOnly = new ArrayList<FeatureRequirement>() {{
            add(FeatureRequirement.Factory.create(Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES, Graph.Features.VertexFeatures.class));
            add(FeatureRequirement.Factory.create(Graph.Features.VertexPropertyFeatures.FEATURE_STRING_VALUES, Graph.Features.VertexPropertyFeatures.class));
            add(FeatureRequirement.Factory.create(Graph.Features.VertexPropertyFeatures.FEATURE_ADD_PROPERTY, Graph.Features.VertexPropertyFeatures.class));
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
