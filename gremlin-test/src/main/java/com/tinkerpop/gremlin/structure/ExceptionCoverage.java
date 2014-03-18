package com.tinkerpop.gremlin.structure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines the list of standard exceptions covered by a test set.  Used in conjunction with the exception compliance
 * unit tests to ensure full coverage of defined exceptions in Blueprints.  The list of exceptions is defined with by
 * the exception class and the list of methods that generate the exceptions that are covered by the test.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(ExceptionCoverageSet.class)
public @interface ExceptionCoverage {
    /**
     * The exception class from Blueprints that contains the exceptions that are covered by the block of tests.
     */
    public Class exceptionClass();

    /**
     * The list of method names from the related {@link #exceptionClass()} that are covered by the tests.
     */
    public String[] methods();
}
