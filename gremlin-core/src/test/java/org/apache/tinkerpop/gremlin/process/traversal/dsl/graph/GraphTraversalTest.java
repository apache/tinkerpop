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
package org.apache.tinkerpop.gremlin.process.traversal.dsl.graph;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphTraversalTest {
    private static final Logger logger = LoggerFactory.getLogger(GraphTraversalTest.class);

    private static Set<String> NO_GRAPH = new HashSet<>(Arrays.asList("asAdmin", "by", "option", "iterate", "to", "from", "profile", "pageRank", "peerPressure", "program"));
    private static Set<String> NO_ANONYMOUS = new HashSet<>(Arrays.asList("start", "__"));
    private static Set<String> IGNORES_BYTECODE = new HashSet<>(Arrays.asList("asAdmin", "iterate", "mapValues", "mapKeys"));

    @Test
    public void shouldHaveMethodsOfGraphTraversalOnAnonymousGraphTraversal() {
        for (Method methodA : GraphTraversal.class.getMethods()) {
            if (Traversal.class.isAssignableFrom(methodA.getReturnType()) && !NO_GRAPH.contains(methodA.getName())) {
                boolean found = false;
                final String methodAName = methodA.getName();
                final String methodAParameters = Arrays.asList(methodA.getParameterTypes()).toString();
                for (final Method methodB : __.class.getMethods()) {
                    final String methodBName = methodB.getName();
                    final String methodBParameters = Arrays.asList(methodB.getParameterTypes()).toString();
                    if (methodAName.equals(methodBName) && methodAParameters.equals(methodBParameters))
                        found = true;
                }
                if (!found)
                    throw new IllegalStateException(__.class.getSimpleName() + " is missing the following method: " + methodAName + ":" + methodAParameters);
            }
        }
    }

    @Test
    public void shouldHaveMethodsOfAnonymousGraphTraversalOnGraphTraversal() {
        for (Method methodA : __.class.getMethods()) {
            if (Traversal.class.isAssignableFrom(methodA.getReturnType()) && !NO_ANONYMOUS.contains(methodA.getName())) {
                boolean found = false;
                final String methodAName = methodA.getName();
                final String methodAParameters = Arrays.asList(methodA.getParameterTypes()).toString();
                for (final Method methodB : GraphTraversal.class.getMethods()) {
                    final String methodBName = methodB.getName();
                    final String methodBParameters = Arrays.asList(methodB.getParameterTypes()).toString();
                    if (methodAName.equals(methodBName) && methodAParameters.equals(methodBParameters))
                        found = true;
                }
                if (!found)
                    throw new IllegalStateException(GraphTraversal.class.getSimpleName() + " is missing the following method: " + methodAName + ":" + methodAParameters);
            }
        }
    }

    @Test
    public void shouldGenerateCorrespondingBytecodeFromGraphTraversalMethods() throws Exception {
        final long seed = System.currentTimeMillis();
        final Random random = new Random(seed);
        logger.info("***RANDOM*** GraphTraversalTest.shouldGenerateCorrespondingBytecodeFromGraphTraversalMethods - seed is {}", seed);

        for (Method stepMethod : GraphTraversal.class.getMethods()) {
            if (Traversal.class.isAssignableFrom(stepMethod.getReturnType()) && !IGNORES_BYTECODE.contains(stepMethod.getName())) {
                final GraphTraversal.Admin<?, ?> traversal = new DefaultGraphTraversal<>();
                Object[] arguments = new Object[stepMethod.getParameterCount()];
                final List<Object> list = new ArrayList<>();
                boolean doTest = true;
                ///
                if (stepMethod.getName().equals("by"))
                    traversal.order();
                else if (stepMethod.getName().equals("option"))
                    traversal.branch(__.identity().out(randomString(random)));
                else if (stepMethod.getName().equals("to") || stepMethod.getName().equals("from"))
                    traversal.addE(randomString(random));
                if (stepMethod.getName().equals("range")) {
                    if (Scope.class.isAssignableFrom(stepMethod.getParameterTypes()[0])) {
                        list.add(arguments[0] = Scope.local);
                        list.add(arguments[1] = (long) (Math.abs(random.nextInt(10))));
                        list.add(arguments[2] = (long) (Math.abs(random.nextInt(10) + 100)));
                    } else {
                        list.add(arguments[0] = (long) (Math.abs(random.nextInt(10))));
                        list.add(arguments[1] = (long) (Math.abs(random.nextInt(10) + 100)));
                    }
                } else if (stepMethod.getName().equals("math")) {
                    list.add(arguments[0] = random.nextInt(100) + " + " + random.nextInt(100));
                } else {
                    for (int i = 0; i < stepMethod.getParameterTypes().length; i++) {
                        final Class<?> type = stepMethod.getParameterTypes()[i];
                        if (int.class.isAssignableFrom(type))
                            list.add(arguments[i] = Math.abs(random.nextInt(100)));
                        else if (long.class.isAssignableFrom(type))
                            list.add(arguments[i] = (long) (Math.abs(random.nextInt(100))));
                        else if (double.class.isAssignableFrom(type))
                            list.add(arguments[i] = Math.abs(random.nextDouble()));
                        else if (String.class.isAssignableFrom(type))
                            list.add(arguments[i] = randomString(random));
                        else if (boolean.class.isAssignableFrom(type))
                            list.add(arguments[i] = random.nextBoolean());
                        else if (String[].class.isAssignableFrom(type)) {
                            arguments[i] = new String[random.nextInt(10) + 1];
                            for (int j = 0; j < ((String[]) arguments[i]).length; j++) {
                                list.add(((String[]) arguments[i])[j] = randomString(random));
                            }
                        } else if (Traversal.class.isAssignableFrom(type))
                            list.add(arguments[i] = __.out(randomString(random)).in(randomString(random)).groupCount(randomString(random)));
                        else if (Traversal[].class.isAssignableFrom(type)) {
                            arguments[i] = new Traversal[random.nextInt(5) + 1];
                            for (int j = 0; j < ((Traversal[]) arguments[i]).length; j++) {
                                list.add(((Traversal[]) arguments[i])[j] = __.as(randomString(random)).out(randomString(random)).both(randomString(random)).has(randomString(random)).store(randomString(random)));
                            }
                        } else if (P.class.isAssignableFrom(type))
                            list.add(arguments[i] = P.gte(stepMethod.getName().contains("where") ? randomString(random) : random.nextInt()));
                        else if (Enum.class.isAssignableFrom(type))
                            list.add(arguments[i] = type.getEnumConstants()[0]);
                        else {
                            // System.out.println(type);
                            doTest = false;
                            break;
                        }
                    }
                }
                if (doTest) {
                    stepMethod.invoke(traversal, arguments);
                    // System.out.print(stepMethod.getName() + "---");
                    final Bytecode.Instruction instruction = traversal.getBytecode().getStepInstructions().get(traversal.getBytecode().getStepInstructions().size() - 1);
                    // System.out.println(instruction);
                    assertEquals(stepMethod.getName(), instruction.getOperator());
                    assertEquals(list.size(), instruction.getArguments().length);
                    for (int i = 0; i < list.size(); i++) {
                        assertEquals(list.get(i) instanceof Traversal ? ((Traversal) list.get(i)).asAdmin().getBytecode() : list.get(i), instruction.getArguments()[i]);
                    }
                }
            }
        }
    }

    private final static String randomString(final Random random) {
        String s = "";
        for (int i = 0; i < random.nextInt(10) + 1; i++) {
            s = (s + (char) (random.nextInt(100) + 1)).trim();
        }
        final String temp = "" + random.nextBoolean();
        return temp.substring(temp.length() - (random.nextInt(2) + 1)) + s.replace("\"", "x").replace(",", "y").replace("'", "z");
    }
}
