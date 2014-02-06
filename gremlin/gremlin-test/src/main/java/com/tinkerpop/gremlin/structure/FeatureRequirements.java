package com.tinkerpop.gremlin.structure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Holds a collection of {@link FeatureRequirement} annotations enabling multiple {@link FeatureRequirement}
 * annotations to be applied to a single test method.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface FeatureRequirements {
    FeatureRequirement[] value();
}
