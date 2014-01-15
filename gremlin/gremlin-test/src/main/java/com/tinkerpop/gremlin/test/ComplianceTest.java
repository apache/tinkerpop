package com.tinkerpop.gremlin.test;

import junit.framework.TestCase;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComplianceTest extends TestCase {

    public void testCompliance() {
        assertTrue(true);
    }

    public static void testCompliance(final Class testClass) {
        Set<String> methodNames = new HashSet<String>();
        for (Method method : testClass.getMethods()) {
            if (method.getDeclaringClass().equals(testClass) && method.getName().startsWith("test")) {
                methodNames.add(method.getName());
            }
        }
        for (Method method : testClass.getMethods()) {
            if (method.getName().startsWith("test")) {
                assertTrue(methodNames.contains(method.getName()));
            }
        }
    }
}
