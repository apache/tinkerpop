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
        assertTrue("Ensure that all test methods from the base class are implemented and that the @Test annotation is appended to each.",
                    Stream.of(gremlinTestBaseClass.getDeclaredMethods())
                            .filter(m -> m.getName().startsWith("test"))
                            .map(m -> {
                                try {
                                    return testClass.getDeclaredMethod(m.getName()).getAnnotation(Test.class) != null;
                                } catch (NoSuchMethodException nsme) {
                                    fail(String.format("Base test method from gremlin-test [%s] not found in test implementation %s",
                                            m.getName(), testClass));

                                    // guess this won't return b/c of above assertion, but something needs to return from
                                    // the clsoure for this to compile.
                                    return false;
                                }
                            }).allMatch(b -> b));
    }
}
