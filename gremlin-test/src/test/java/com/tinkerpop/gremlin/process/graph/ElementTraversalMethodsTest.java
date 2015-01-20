package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementTraversalMethodsTest {

    @Test
    public void shouldHaveAllGraphTraversalMethodsOffVertex() {
        final List<Method> graphTraversalMethods = Arrays.asList(GraphTraversal.class.getMethods()).stream()
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> !m.getName().equals("addStep"))
                .filter(m -> !m.getName().equals("value"))
                .filter(m -> !m.getName().equals("id"))
                .filter(m -> !m.getName().equals("label"))
                .filter(m -> !m.getName().equals("key"))
                .filter(m -> !m.getName().equals("by"))
                .filter(m -> GraphTraversal.class.isAssignableFrom(m.getReturnType())).collect(Collectors.toList());

        final List<Method> vertexMethods = new ArrayList<>(Arrays.asList(Vertex.class.getMethods()));

        final List<Method> nonExistent = graphTraversalMethods.stream()
                .filter(m -> !existsInList(m, vertexMethods))
                .collect(Collectors.toList());
        if (nonExistent.size() > 0) {
            for (Method method : nonExistent) {
                System.out.println("Requirement implementation: " + method);
            }
            fail("The following methods are not implemented by Vertex: " + nonExistent);
        }
    }

    @Test
    public void shouldHaveAllGraphTraversalMethodsOffEdge() {
        final List<Method> graphTraversalMethods = Arrays.asList(GraphTraversal.class.getMethods()).stream()
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> !m.getName().equals("addStep"))
                .filter(m -> !m.getName().equals("value"))
                .filter(m -> !m.getName().equals("id"))
                .filter(m -> !m.getName().equals("label"))
                .filter(m -> !m.getName().equals("key"))
                .filter(m -> !m.getName().equals("by"))
                .filter(m -> GraphTraversal.class.isAssignableFrom(m.getReturnType())).collect(Collectors.toList());

        final List<Method> edgeMethods = new ArrayList<>(Arrays.asList(Edge.class.getMethods()));

        final List<Method> nonExistent = graphTraversalMethods.stream()
                .filter(m -> !existsInList(m, edgeMethods))
                .collect(Collectors.toList());
        if (nonExistent.size() > 0) {
            for (Method method : nonExistent) {
                System.out.println("Requirement implementation: " + method);
            }
            fail("The following methods are not implemented by Edge: " + nonExistent);
        }

    }

    @Test
    public void shouldHaveAllGraphTraversalMethodsOffVertexProperty() {
        final List<Method> graphTraversalMethods = Arrays.asList(GraphTraversal.class.getMethods()).stream()
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> !m.getName().equals("addStep"))
                .filter(m -> !m.getName().equals("value"))
                .filter(m -> !m.getName().equals("id"))
                .filter(m -> !m.getName().equals("label"))
                .filter(m -> !m.getName().equals("key"))
                .filter(m -> !m.getName().equals("by"))
                .filter(m -> GraphTraversal.class.isAssignableFrom(m.getReturnType())).collect(Collectors.toList());

        final List<Method> vertexPropertyMethods = new ArrayList<>(Arrays.asList(VertexProperty.class.getMethods()));

        final List<Method> nonExistent = graphTraversalMethods.stream()
                .filter(m -> !existsInList(m, vertexPropertyMethods))
                .collect(Collectors.toList());
        if (nonExistent.size() > 0) {
            for (Method method : nonExistent) {
                System.out.println("Requirement implementation: " + method);
            }
            fail("The following methods are not implemented by Edge: " + nonExistent);
        }

    }


    private static boolean existsInList(final Method method, final List<Method> methods) {
        final List<Method> nonMatches = methods.stream()
                .filter(m -> m.getName().equals(method.getName()))
                .filter(m -> m.getReturnType().equals(method.getReturnType()))
                .filter(m -> m.getParameterCount() == method.getParameterCount())
                .filter(m -> {
                    boolean equals = true;
                    for (int i = 0; i < m.getParameters().length; i++) {
                        if (!m.getParameters()[i].getType().equals(method.getParameters()[i].getType())) {
                            equals = false;
                        }
                    }
                    return equals;
                })
                .collect(Collectors.toList());
        return nonMatches.size() == 1;
    }
}
