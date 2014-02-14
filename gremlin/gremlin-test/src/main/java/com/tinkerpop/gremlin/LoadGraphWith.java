package com.tinkerpop.gremlin;

/**
 * Annotations to define a graph example to load from test resources prior to test execution.  This annotation is
 * for use only with test that extend from {@link AbstractToyGraphGremlinTest}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public @interface LoadGraphWith {

    public enum GraphData {
        CLASSIC,
        GRATEFUL;


        public String location() {
            switch (this) {
                case CLASSIC:
                    return RESOURCE_PATH_PREFIX + "graph-example-1.xml";
                case GRATEFUL:
                    return RESOURCE_PATH_PREFIX + "graph-example-2.xml";
            }

            throw new RuntimeException("No file for this GraphData type");
        }
    }

    public static final String RESOURCE_PATH_PREFIX = "/com/tinkerpop/gremlin/structure/util/io/graphml/";

    /**
     * The name of the resource to load with full path.
     */
    public GraphData value() default GraphData.CLASSIC;
}
