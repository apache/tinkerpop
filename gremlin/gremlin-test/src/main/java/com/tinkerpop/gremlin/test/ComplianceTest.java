package com.tinkerpop.gremlin.test;

import org.junit.Test;

import java.util.stream.Stream;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ComplianceTest {

    @Test
    public void testCompliance() {
        assertTrue(true);
    }

    public static void testCompliance(final Class testClass) {
        // get the base class from gremlin-test
        final Class gremlinTestBaseClass = testClass.getSuperclass();
        assertNotNull(gremlinTestBaseClass);

        // get the test methods from base and validate that they are somehow implemented in the parent class
        // and have a junit @Test annotation.
        assertTrue(Stream.of(gremlinTestBaseClass.getDeclaredMethods())
                .map(m -> {
                    try {
                        assertNotNull(testClass.getDeclaredMethod(m.getName()).getAnnotation(Test.class));
                        return true;
                    } catch (final NoSuchMethodException e) {
                        fail(String.format("Base test method from gremlin-test [%s] not found in test implementation %s",
                                m.getName(), testClass));
                        return false;
                    }
                }).reduce(true, (a, b) -> a && b));
    }
}
