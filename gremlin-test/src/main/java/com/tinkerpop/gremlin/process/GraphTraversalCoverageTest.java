package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import org.javatuples.Pair;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphTraversalCoverageTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(CLASSIC)
    public void shouldHaveSameTraversalReturnTypesForAllMethods() {
        final Set<String> allReturnTypes = new HashSet<>();
        final List<Pair<String, Object>> vendorsClasses = Arrays.asList(
                Pair.<String, Object>with("g.V Traversal", g.V()),
                Pair.<String, Object>with("g.E Traversal", g.E()),
                Pair.<String, Object>with("v.identity Traversal", g.V().next().identity()),
                Pair.<String, Object>with("e.identity Traversal", g.E().next().identity()),
                Pair.<String, Object>with("v", g.V().next()),
                Pair.<String, Object>with("e", g.E().next())
                //Pair.<String, Object>with("__", AnonymousGraphTraversal.Tokens.__)
        );
        vendorsClasses.forEach(triplet -> {
            final String situation = triplet.getValue0();
            final Class vendorClass = triplet.getValue1().getClass();
            final Set<String> returnTypes = new HashSet<>();
            Arrays.asList(vendorClass.getMethods())
                    .stream()
                    .filter(m -> GraphTraversal.class.isAssignableFrom(m.getReturnType()))
                    .filter(m -> !Modifier.isStatic(m.getModifiers()))
                    .map(m -> getDeclaredVersion(m, vendorClass))
                    .forEach(m -> {
                        returnTypes.add(m.getReturnType().getCanonicalName());
                        allReturnTypes.add(m.getReturnType().getCanonicalName());
                    });
            if (returnTypes.size() > 1) {
                final boolean muted = Boolean.parseBoolean(System.getProperty("muteTestLogs", "false"));
                if (!muted)
                    System.out.println("FAILURE: " + vendorClass.getCanonicalName() + " methods do not return in full fluency for [" + situation + "]");
                fail("The return types of all traversal methods should be the same to ensure proper fluency: " + returnTypes);
            } else {
                final boolean muted = Boolean.parseBoolean(System.getProperty("muteTestLogs", "false"));
                if (!muted)
                    System.out.println("SUCCESS: " + vendorClass.getCanonicalName() + " methods return in full fluency for [" + situation + "]");
            }
        });
        if (allReturnTypes.size() > 1)
            fail("All traversals possible do not maintain the same return types and thus, not fully fluent for entire graph system: " + allReturnTypes);
    }

    private static Method getDeclaredVersion(final Method method, final Class vendorClass) {
        return Arrays.asList(vendorClass.getMethods())
                .stream()
                .filter(m -> !GraphTraversal.class.equals(m.getReturnType()))
                .filter(m -> !Traversal.class.equals(m.getReturnType()))
                        //.filter(m -> !Traversal.class.isAssignableFrom(m.getReturnType()))
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> m.getName().equals(method.getName()))
                .filter(m -> Arrays.asList(m.getParameterTypes()).toString().equals(Arrays.asList(method.getParameterTypes()).toString()))
                .findAny().orElseGet(() -> {
                    final boolean muted = Boolean.parseBoolean(System.getProperty("muteTestLogs", "false"));
                    if (!muted)
                        System.out.println("IGNORE IF TEST PASSES: Can not find native implementation of: " + method);
                    return method;
                });
    }

}
