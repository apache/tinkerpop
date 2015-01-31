package com.tinkerpop.gremlin;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;

import static com.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.*;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES;

/**
 * Annotations to define a graph example to load from test resources prior to test execution.  This annotation is
 * for use only with test that extend from {@link AbstractGremlinTest}.
 * <br/>
 * Note that the annotation assumes that the @{link GraphData} referenced by the annotation can be made available
 * to the graph in the test.  In other words, even a graph that has "read-only" features, should have some method of
 * getting the data available to it.  That said, graphs must minimally support the data types that the sample
 * data contains for the test to be executed.
 * <br/>
 * If a graph implementation is "read-only", it can override the
 * {@link GraphProvider#loadGraphData(com.tinkerpop.gremlin.structure.Graph, LoadGraphWith, Class, String)} method
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
         * Loads the "modern" TinkerPop toy graph which is like "classic", but with the "weight" value on edges stored
         * as double and labels added for vertices.  This should be the most commonly used graph instance for testing
         * as graphs that support string, double and int should comprise the largest number of implementations.
         */
        MODERN,

        /**
         * Load "The Crew" TinkerPop3 toy graph which includes {@link com.tinkerpop.gremlin.structure.VertexProperty} data.
         */
        CREW,

        /**
         * Loads the "grateful dead" graph which is a "large" graph which provides for the construction of more
         * complex traversals.
         */
        GRATEFUL;

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

        private static final List<FeatureRequirement> featuresRequiredByClassicDouble = new ArrayList<FeatureRequirement>() {{
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
                    return RESOURCE_PATH_PREFIX + "tinkerpop-classic.gio";
                case CREW:
                    return RESOURCE_PATH_PREFIX + "tinkerpop-crew.gio";
                case MODERN:
                    return RESOURCE_PATH_PREFIX + "tinkerpop-modern.gio";
                case GRATEFUL:
                    return RESOURCE_PATH_PREFIX + "grateful-dead.gio";
            }

            throw new RuntimeException("No file for this GraphData type");
        }

        public List<FeatureRequirement> featuresRequired() {
            switch (this) {
                case CLASSIC:
                    return featuresRequiredByClassic;
                case CREW:
                    return featuresRequiredByCrew;
                case MODERN:
                    return featuresRequiredByClassicDouble;
                case GRATEFUL:
                    return featuresRequiredByGrateful;
            }

            throw new RuntimeException("No features for this GraphData type");
        }
    }

    public static final String RESOURCE_PATH_PREFIX = "/com/tinkerpop/gremlin/structure/io/kryo/";

    /**
     * The name of the resource to load with full path.
     */
    public GraphData value() default GraphData.CLASSIC;

}
