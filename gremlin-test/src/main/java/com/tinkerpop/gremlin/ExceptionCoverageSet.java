package com.tinkerpop.gremlin;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A group of {@link ExceptionCoverage} sideEffects.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ExceptionCoverageSet {
    ExceptionCoverage[] value();
}
