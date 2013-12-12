package com.tinkerpop.blueprints.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A annotation for feature methods.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface FeatureDescriptor {
    /**
     * The name of the feature which is represented as the text of the method it is annotating after the "supports"
     * prefix
     */
    String name();

    /**
     * A description of the feature that will be useful to implementers if a Blueprints test fails around
     * proper feature support.
     */
    String description() default "";
}
