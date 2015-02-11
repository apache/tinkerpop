/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.apache.tinkerpop.gremlin.process.graph.traversal;

import com.apache.tinkerpop.gremlin.process.graph.traversal.GraphTraversal;
import com.apache.tinkerpop.gremlin.structure.Edge;
import com.apache.tinkerpop.gremlin.structure.Vertex;
import com.apache.tinkerpop.gremlin.structure.VertexProperty;
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
public class GraphTraversalMethodsTest {

    @Test
    public void shouldHaveAllGraphTraversalMethodsOff__() {
        final List<Method> graphTraversalMethods = Arrays.asList(GraphTraversal.class.getMethods()).stream()
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> !m.getName().equals("by"))
                .filter(m -> !m.getName().equals("option"))
                .filter(m -> !m.getName().equals("asAdmin"))
                .filter(m -> GraphTraversal.class.isAssignableFrom(m.getReturnType())).collect(Collectors.toList());

        final List<Method> vertexMethods = new ArrayList<>(Arrays.asList(__.class.getMethods()));

        final List<Method> nonExistent = graphTraversalMethods.stream()
                .filter(m -> !existsInList(m, vertexMethods))
                .collect(Collectors.toList());
        if (nonExistent.size() > 0) {
            for (Method method : nonExistent) {
                System.out.println("Requirement implementation: " + method);
            }
            fail("The following methods are not implemented by __: " + nonExistent);
        }
    }

    @Test
    public void shouldHaveAllGraphTraversalMethodsOffVertex() {
        final List<Method> graphTraversalMethods = Arrays.asList(GraphTraversal.class.getMethods()).stream()
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> !m.getName().equals("value"))
                .filter(m -> !m.getName().equals("id"))
                .filter(m -> !m.getName().equals("label"))
                .filter(m -> !m.getName().equals("key"))
                .filter(m -> !m.getName().equals("by"))
                .filter(m -> !m.getName().equals("option"))
                .filter(m -> !m.getName().equals("asAdmin"))
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
                .filter(m -> !m.getName().equals("value"))
                .filter(m -> !m.getName().equals("id"))
                .filter(m -> !m.getName().equals("label"))
                .filter(m -> !m.getName().equals("key"))
                .filter(m -> !m.getName().equals("by"))
                .filter(m -> !m.getName().equals("option"))
                .filter(m -> !m.getName().equals("asAdmin"))
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
                .filter(m -> !m.getName().equals("value"))
                .filter(m -> !m.getName().equals("id"))
                .filter(m -> !m.getName().equals("label"))
                .filter(m -> !m.getName().equals("key"))
                .filter(m -> !m.getName().equals("by"))
                .filter(m -> !m.getName().equals("option"))
                .filter(m -> !m.getName().equals("asAdmin"))
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
