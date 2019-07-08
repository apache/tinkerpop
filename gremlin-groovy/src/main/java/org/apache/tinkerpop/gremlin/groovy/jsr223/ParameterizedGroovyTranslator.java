/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.groovy.jsr223;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.Translator.ScriptTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Converts bytecode to a Parameterized Groovy string of Gremlin.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Stark Arya (sandszhou.zj@alibaba-inc.com)
 */

public class ParameterizedGroovyTranslator implements ScriptTranslator {
    private final String traversalSource;
    private final TypeTranslator typeTranslator;

    private ParameterizedGroovyTranslator(final String traversalSource, TypeTranslator typeTranslator) {
        this.traversalSource = traversalSource;
        this.typeTranslator = typeTranslator;
    }

    public static final ParameterizedGroovyTranslator of(final String traversalSource) {
        return of(traversalSource,null);
    }

    public static final ParameterizedGroovyTranslator of(final String traversalSource, final TypeTranslator typeTranslator) {
        return new ParameterizedGroovyTranslator(traversalSource,
                Optional.ofNullable(typeTranslator).orElseGet(DefaultParameterizedTypeTranslator::new));
    }

    //////
    public ParameterizedGroovyResult parameterizedTranslate(final Bytecode bytecode) {
        String groovy = typeTranslator.apply(traversalSource, bytecode).toString();

        Map<String, Object> parameters = (typeTranslator instanceof  DefaultParameterizedTypeTranslator) ?
                ((DefaultParameterizedTypeTranslator) typeTranslator).getParameters() : new HashMap<>(0);

        return new ParameterizedGroovyResult(groovy, parameters);
    }

    @Override
    public String translate(final Bytecode bytecode) {
        return typeTranslator.apply(traversalSource, bytecode).toString();
    }

    @Override
    public String getTargetLanguage() {
        return "gremlin-groovy";
    }

    @Override
    public String toString() {
        return StringFactory.translatorString(this);
    }

    @Override
    public String getTraversalSource() {
        return this.traversalSource;
    }

    /**
     * Parameterized Script Result
     */
    public final class ParameterizedGroovyResult {
        private final String  script;
        private final Map<String,Object> parameters;

        public ParameterizedGroovyResult(String script, Map<String,Object> parameters) {
            this.script = script;
            this.parameters = Collections.unmodifiableMap(parameters);
        }

        public String getScript() {
            return script;
        }

        public Map<String,Object> getParameters() {
            return parameters;
        }
    }

    /**
     * Performs standard type translation for the TinkerPop types to Parameterized Groovy.
     */
    public static class DefaultParameterizedTypeTranslator implements TypeTranslator {
        private Parameters parameters = Parameters.instance();

        @Override
        public Object apply(final String traversalSource, final Object o) {
            if (o instanceof Bytecode) {
                return internalTranslate(traversalSource, (Bytecode) o);
            } else {
                return  convertToString(o);
            }
        }

        public Map<String, Object> getParameters() {
            return parameters.getParametersAndClear();
        }


        /**
         *  For each operator argument, try parametrization
         *
         *  -----------------------------------------------
         *  if unpack, why ?     ObjectType
         *  -----------------------------------------------
         * （Yes）                Bytecode.Binding
         * （Recursion, No）      Bytecode
         *  (Recursion, No）      Traversal
         * （Yes）                String
         * （Recursion, No）      Set
         * （Recursion, No）      List
         * （Recursion, No）      Map
         * （Yes）                Long
         * （Yes）                Double
         * （Yes）                Float
         * （Yes）                Integer
         * （Recursion, No）      P
         * （Enumeration, No）    SackFunctions.Barrier
         * （Enumeration, No）    VertexProperty.Cardinality
         * （Enumeration, No）    TraversalOptionParent.Pick
         * （Enumeration, No）    Enum
         * （Recursion, No）      Vertex
         * （Recursion, No）      Edge
         * （Recursion, No）      VertexProperty
         * （Yes）                Lambda
         * （Recursion, No）      TraversalStrategyProxy
         * （Enumeration, No）    TraversalStrategy
         *  (Yes)                 Other
         *  -------------------------------------------------
         * @param object
         * @return String Repres
         */
        private String convertToString(final Object object) {
            if (object instanceof Bytecode.Binding) {
                return parameters.getBoundKeyOrAssign(((Bytecode.Binding) object).variable());
            } else if (object instanceof Bytecode) {
                return this.internalTranslate("__", (Bytecode) object);
            } else if (object instanceof Traversal) {
                return convertToString(((Traversal) object).asAdmin().getBytecode());
            } else if (object instanceof String) {
                return parameters.getBoundKeyOrAssign(object);
            } else if (object instanceof Set) {
                final Set<String> set = new HashSet<>(((Set) object).size());
                for (final Object item : (Set) object) {
                    set.add(convertToString(item));
                }
                return set.toString() + " as Set";
            } else if (object instanceof List) {
                final List<String> list = new ArrayList<>(((List) object).size());
                for (final Object item : (List) object) {
                    list.add(convertToString(item));
                }
                return list.toString();
            } else if (object instanceof Map) {
                final StringBuilder map = new StringBuilder("[");
                for (final Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                    map.append("(").
                            append(convertToString(entry.getKey())).
                            append("):(").
                            append(convertToString(entry.getValue())).
                            append("),");
                }

                // only need to remove this last bit if entries were added
                if (!((Map<?, ?>) object).isEmpty())
                    map.deleteCharAt(map.length() - 1);

                return map.append("]").toString();
            } else if (object instanceof Long) {
                return parameters.getBoundKeyOrAssign(object);
            } else if (object instanceof Double) {
                return parameters.getBoundKeyOrAssign(object);
            } else if (object instanceof Float) {
                return parameters.getBoundKeyOrAssign(object);
            } else if (object instanceof Integer) {
                return parameters.getBoundKeyOrAssign(object);
            } else if (object instanceof Class) {
                return ((Class) object).getCanonicalName();
            } else if (object instanceof P) {
                return convertPToString((P) object, new StringBuilder()).toString();
            } else if (object instanceof SackFunctions.Barrier) {
                return "SackFunctions.Barrier." + object.toString();
            } else if (object instanceof VertexProperty.Cardinality) {
                return "VertexProperty.Cardinality." + object.toString();
            } else if (object instanceof TraversalOptionParent.Pick) {
                return "TraversalOptionParent.Pick." + object.toString();
            } else if (object instanceof Enum) {
                return ((Enum) object).getDeclaringClass().getSimpleName() + "." + object.toString();
            } else if (object instanceof Element) {
                if (object instanceof Vertex) {
                    final Vertex vertex = (Vertex) object;
                    return "new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(" +
                            convertToString(vertex.id()) + "," +
                            convertToString(vertex.label()) + ", Collections.emptyMap())";
                } else if (object instanceof Edge) {
                    final Edge edge = (Edge) object;
                    return "new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge(" +
                            convertToString(edge.id()) + "," +
                            convertToString(edge.label()) + "," +
                            "Collections.emptyMap()," +
                            convertToString(edge.outVertex().id()) + "," +
                            convertToString(edge.outVertex().label()) + "," +
                            convertToString(edge.inVertex().id()) + "," +
                            convertToString(edge.inVertex().label()) + ")";
                } else {// VertexProperty
                    final VertexProperty vertexProperty = (VertexProperty) object;
                    return "new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty(" +
                            convertToString(vertexProperty.id()) + "," +
                            convertToString(vertexProperty.label()) + "," +
                            convertToString(vertexProperty.value()) + "," +
                            "Collections.emptyMap()," +
                            convertToString(vertexProperty.element()) + ")";
                }
            } else if (object instanceof Lambda) {
                return parameters.getBoundKeyOrAssign(object);
            } else if (object instanceof TraversalStrategyProxy) {
                final TraversalStrategyProxy proxy = (TraversalStrategyProxy) object;
                if (proxy.getConfiguration().isEmpty()) {
                    return proxy.getStrategyClass().getCanonicalName() + ".instance()";
                } else {
                    return proxy.getStrategyClass().getCanonicalName() + ".create(new org.apache.commons.configuration.MapConfiguration(" + convertToString(ConfigurationConverter.getMap(proxy.getConfiguration())) + "))";
                }
            } else if (object instanceof TraversalStrategy) {
                return convertToString(new TraversalStrategyProxy(((TraversalStrategy) object)));
            } else {
                return null == object ? "null" : parameters.getBoundKeyOrAssign(object);
            }
        }

        private String internalTranslate(final String start, final Bytecode bytecode) {
            final StringBuilder traversalScript = new StringBuilder(start);
            for (final Bytecode.Instruction instruction : bytecode.getInstructions()) {
                final String methodName = instruction.getOperator();
                if (0 == instruction.getArguments().length) {
                    traversalScript.append(".").append(methodName).append("()");
                } else {
                    traversalScript.append(".");
                    String temp = methodName + "(";
                    for (final Object object : instruction.getArguments()) {
                        temp = temp + convertToString(object) + ",";
                    }
                    traversalScript.append(temp.substring(0, temp.length() - 1)).append(")");
                }
            }

            return traversalScript.toString();
        }

        private StringBuilder convertPToString(final P p, final StringBuilder current) {
            if (p instanceof ConnectiveP) {
                final List<P<?>> list = ((ConnectiveP) p).getPredicates();
                for (int i = 0; i < list.size(); i++) {
                    convertPToString(list.get(i), current);
                    if (i < list.size() - 1) {
                        current.append(p instanceof OrP ? ".or(" : ".and(");
                    }
                }
                current.append(")");
            } else {
                current.append("P.").append(p.getBiPredicate().toString()).append("(").append(convertToString(p.getValue())).append(")");
            }
            return current;
        }

        public static final class Parameters {
            private static final String KEY_PREFIX = "_args_";
            private static final Parameters INSTANCE = new Parameters();
            private static final ThreadLocal<Map<Object, String>> MAP = ThreadLocal.withInitial(HashMap::new);

            private Parameters() {}

            public <V> String getBoundKeyOrAssign(final V value) {
                final Map<Object, String> map = MAP.get();
                if (!map.containsKey(value)) {
                    map.put(value, getNextBoundKey());
                }
                return map.get(value);
            }

            public Map<String, Object> getParametersAndClear() {
                Map<String, Object> result = MAP.get().entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
                MAP.get().clear();
                return result;
            }

            public static Parameters instance() {
                return INSTANCE;
            }

            /**
             * @return  a monotonically increasing key
             */
            private String getNextBoundKey() {
                return KEY_PREFIX + String.valueOf(MAP.get().size());
            }

            @Override
            public String toString() {
                return "parameters[" + Thread.currentThread().getName() + "]";
            }
        }
    }
}