package com.tinkerpop.gremlin;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotations to define a graph example to load from test resources prior to test execution.  This annotation is
 * for use only with test that extend from {@link AbstractGremlinTest}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface LoadGraphWith {

    public enum GraphData {
        CLASSIC,
        GRATEFUL,
        MODERN;


        public String location() {
            switch (this) {
                case CLASSIC:
                    return RESOURCE_PATH_PREFIX + "tinkerpop-classic.gio";
                case GRATEFUL:
                    return RESOURCE_PATH_PREFIX + "grateful-dead.gio";
                case MODERN:
                    return RESOURCE_PATH_PREFIX + "tinkerpop-modern.gio";
            }

            throw new RuntimeException("No file for this GraphData type");
        }
    }

    public static final String RESOURCE_PATH_PREFIX = "/com/tinkerpop/gremlin/structure/util/io/kryo/";

    /**
     * The name of the resource to load with full path.
     */
    public GraphData value() default GraphData.CLASSIC;
}
