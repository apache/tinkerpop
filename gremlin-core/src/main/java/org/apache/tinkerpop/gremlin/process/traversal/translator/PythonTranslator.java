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

package org.apache.tinkerpop.gremlin.process.traversal.translator;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Script;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Translates Gremlin {@link Bytecode} into a Python string representation.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class PythonTranslator implements Translator.ScriptTranslator {

    private static final Set<String> STEP_NAMES = Stream.of(GraphTraversal.class.getMethods()).filter(
            method -> Traversal.class.isAssignableFrom(method.getReturnType())).map(Method::getName).
            collect(Collectors.toSet());

    private final String traversalSource;
    private final TypeTranslator typeTranslator;

    PythonTranslator(final String traversalSource, final TypeTranslator typeTranslator) {
        this.traversalSource = traversalSource;
        this.typeTranslator = typeTranslator;
    }

    public static PythonTranslator of(final String traversalSource, final boolean withParameters) {
        return of(traversalSource, new DefaultTypeTranslator(withParameters));
    }

    public static PythonTranslator of(final String traversalSource) {
        return of(traversalSource, false);
    }

    public static PythonTranslator of(final String traversalSource, final TypeTranslator typeTranslator) {
        return new PythonTranslator(traversalSource, typeTranslator);
    }

    @Override
    public String getTraversalSource() {
        return this.traversalSource;
    }

    @Override
    public Script translate(final Bytecode bytecode) {
        return typeTranslator.apply(traversalSource, bytecode);
    }

    @Override
    public String getTargetLanguage() {
        return "gremlin-python";
    }

    @Override
    public String toString() {
        return StringFactory.translatorString(this);
    }

    ///////

    /**
     * Performs standard type translation for the TinkerPop types to Python.
     */
    public static class DefaultTypeTranslator implements TypeTranslator {
        protected final boolean withParameters;
        protected final Script script;

        public DefaultTypeTranslator(final boolean withParameters) {
            this.withParameters = withParameters;
            this.script = new Script();
        }

        @Override
        public Script apply(final String traversalSource, final Object o) {
            this.script.init();
            if (o instanceof Bytecode) {
                return internalTranslate(traversalSource, (Bytecode) o);
            } else {
                return convertToScript(o);
            }
        }

        /**
         * For each operator argument, if withParameters set true, try parametrization as follows:
         * <p>
         * -----------------------------------------------
         * if unpack, why ?     ObjectType
         * -----------------------------------------------
         * （Yes）                Bytecode.Binding
         * （Recursion, No）      Bytecode
         * (Recursion, No）      Traversal
         * （Yes）                String
         * （Recursion, No）      Set
         * （Recursion, No）      List
         * （Recursion, No）      Map
         * （Yes）                Long
         * （Yes）                Double
         * （Yes）                Float
         * （Yes）                Integer
         * （Yes）                Timestamp
         * （Yes）                Date
         * （Yes）                Uuid
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
         * (Yes)                 Other
         * -------------------------------------------------
         *
         * @param object
         * @return String Repres
         */
        protected Script convertToScript(final Object object) {
            if (object instanceof Bytecode.Binding)
                return script.getBoundKeyOrAssign(withParameters, ((Bytecode.Binding) object).variable());
            else if (object instanceof Bytecode)
                return this.internalTranslate("__", (Bytecode) object);
            else if (object instanceof Traversal)
                return convertToScript(((Traversal) object).asAdmin().getBytecode());
            else if (object instanceof String) {
                final String wrapper = ((String) object).contains("'") || ((String) object).contains(System.lineSeparator()) ?
                        "\"\"\"" + object + "\"\"\"" : "'" + object + "'";
                return script.getBoundKeyOrAssign(withParameters, withParameters ? object : wrapper);
            } else if (object instanceof Set) {
                final Iterator<?> iterator = ((Set) object).iterator();
                script.append("set(");
                while(iterator.hasNext()) {
                    convertToScript(iterator.next());
                    if (iterator.hasNext())
                        script.append(",");
                }
                return script.append(")");
            } else if (object instanceof List) {
                final Iterator<?> iterator = ((List) object).iterator();
                script.append("[");
                while(iterator.hasNext()) {
                    convertToScript(iterator.next());
                    if (iterator.hasNext())
                        script.append(",");
                }
                return script.append("]");
            } else if (object instanceof Map) {
                script.append("{");
                final Iterator<? extends Map.Entry<?, ?>> itty = ((Map<?, ?>) object).entrySet().iterator();
                while (itty.hasNext()) {
                    final Map.Entry<?,?> entry = itty.next();
                    convertToScript(entry.getKey()).append(":");
                    convertToScript(entry.getValue());
                    if (itty.hasNext())
                        script.append(",");
                }
                return script.append("}");
            } else if (object instanceof Long)
                return script.getBoundKeyOrAssign(withParameters, withParameters ? object : "long(" + object + ")");
            else if (object instanceof TraversalStrategyProxy) {
                return resolveTraversalStrategyProxy((TraversalStrategyProxy) object);
            } else if (object instanceof TraversalStrategy) {
                return convertToScript(new TraversalStrategyProxy((TraversalStrategy) object));
            } else if (object instanceof Boolean)
                return script.getBoundKeyOrAssign(withParameters, withParameters ? object : object.equals(Boolean.TRUE) ? "True" : "False");
            else if (object instanceof Class)
                return script.append(((Class) object).getCanonicalName());
            else if (object instanceof VertexProperty.Cardinality)
                return script.append("Cardinality." + resolveSymbol(object.toString()));
            else if (object instanceof SackFunctions.Barrier)
                return script.append("Barrier." + resolveSymbol(object.toString()));
            else if (object instanceof TraversalOptionParent.Pick)
                return script.append("Pick." + resolveSymbol(object.toString()));
            else if (object instanceof Enum)
                return script.append(((Enum) object).getDeclaringClass().getSimpleName() + "." + resolveSymbol(object.toString()));
            else if (object instanceof P)
                return convertPToScript((P) object);
            else if (object instanceof Element) {
                if (object instanceof Vertex) {
                    final Vertex vertex = (Vertex) object;
                    script.append("Vertex(");
                    convertToScript(vertex.id()).append(",");
                    return convertToScript(vertex.label()).append(")");
                } else if (object instanceof Edge) {
                    final Edge edge = (Edge) object;
                    script.append("Edge(");
                    convertToScript(edge.id()).append(",");
                    convertToScript(edge.outVertex()).append(",");
                    convertToScript(edge.label()).append(",");
                    return convertToScript(edge.inVertex()).append(")");
                } else {
                    final VertexProperty vertexProperty = (VertexProperty) object;
                    script.append("VertexProperty(");
                    convertToScript(vertexProperty.id()).append(",");
                    convertToScript(vertexProperty.label()).append(",");
                    return convertToScript(vertexProperty.value()).append(")");
                }
            } else if (object instanceof Lambda) {
                final String lambdaString = ((Lambda) object).getLambdaScript().trim();
                final String wrapper = lambdaString.startsWith("lambda") ? lambdaString : "lambda " + lambdaString;;
                return script.getBoundKeyOrAssign(withParameters, withParameters ? object : wrapper);
            } else
                return null == object ? script.append("None") : script.getBoundKeyOrAssign(withParameters, object);
        }

        private Script internalTranslate(final String start, final Bytecode bytecode) {
            script.append(start);
            for (final Bytecode.Instruction instruction : bytecode.getInstructions()) {
                final String methodName = instruction.getOperator();
                final Object[] arguments = instruction.getArguments();
                if (0 == arguments.length)
                    script.append(".").append(resolveSymbol(methodName)).append("()");
                else if (methodName.equals("range") && 2 == arguments.length)
                    if (((Number) arguments[0]).longValue() + 1 == ((Number) arguments[1]).longValue())
                        script.append("[").append(arguments[0].toString()).append("]");
                    else
                        script.append("[").append(arguments[0].toString()).append(":").append(arguments[1].toString()).append("]");
                else if (methodName.equals("limit") && 1 == arguments.length)
                    script.append("[0:").append(arguments[0].toString()).append("]");
                else if (methodName.equals("values") && 1 == arguments.length && script.getScript().length() > 3 && !STEP_NAMES.contains(arguments[0].toString()))
                    script.append(".").append(arguments[0].toString());
                else {
                    script.append(".").append(resolveSymbol(methodName)).append("(");

                    // python has trouble with java varargs...wrapping in collection seems to solve the problem
                    final boolean varargsBeware = instruction.getOperator().equals(TraversalSource.Symbols.withStrategies)
                            || instruction.getOperator().equals(TraversalSource.Symbols.withoutStrategies);
                    if (varargsBeware) script.append("[");

                    final Iterator<?> itty = Stream.of(arguments).iterator();
                    while (itty.hasNext()) {
                        convertToScript(itty.next());
                        if (itty.hasNext()) script.append(",");
                    }

                    if (varargsBeware) script.append("]");

                    script.append(")");
                }
            }
            return script;
        }

        protected Script convertPToScript(final P p) {
            if (p instanceof TextP) {
                return convertTextPToScript((TextP) p);
            }
            if (p instanceof ConnectiveP) {
                final List<P<?>> list = ((ConnectiveP) p).getPredicates();
                for (int i = 0; i < list.size(); i++) {
                    convertPToScript(list.get(i));
                    if (i < list.size() - 1) {
                        script.append(p instanceof OrP ? ".or_(" : ".and_(");
                    }
                }
                script.append(")");
            } else {
                script.append("P.").append(p.getBiPredicate().toString()).append("(");
                convertToScript(p.getValue());
                script.append(")");
            }
            return script;
        }

        protected Script convertTextPToScript(final TextP p) {
            script.append("TextP.").append(p.getBiPredicate().toString()).append("(");
            convertToScript(p.getValue());
            script.append(")");
            return script;
        }

        protected String resolveSymbol(final String methodName) {
            return SymbolHelper.toPython(methodName);
        }

        protected Script resolveTraversalStrategyProxy(final TraversalStrategyProxy proxy) {
            if (proxy.getConfiguration().isEmpty())
                return script.append("TraversalStrategy('" + proxy.getStrategyClass().getSimpleName() + "')");
            else {
                script.append("TraversalStrategy('").append(proxy.getStrategyClass().getSimpleName()).append("',");
                convertToScript(ConfigurationConverter.getMap(proxy.getConfiguration()));
                return script.append(")");
            }
        }
    }

    static final class SymbolHelper {

        private final static Map<String, String> TO_PYTHON_MAP = new HashMap<>();
        private final static Map<String, String> FROM_PYTHON_MAP = new HashMap<>();

        static {
            TO_PYTHON_MAP.put("global", "global_");
            TO_PYTHON_MAP.put("as", "as_");
            TO_PYTHON_MAP.put("in", "in_");
            TO_PYTHON_MAP.put("and", "and_");
            TO_PYTHON_MAP.put("or", "or_");
            TO_PYTHON_MAP.put("is", "is_");
            TO_PYTHON_MAP.put("not", "not_");
            TO_PYTHON_MAP.put("from", "from_");
            TO_PYTHON_MAP.put("list", "list_");
            TO_PYTHON_MAP.put("set", "set_");
            TO_PYTHON_MAP.put("all", "all_");
            TO_PYTHON_MAP.put("with", "with_");
            //
            TO_PYTHON_MAP.forEach((k, v) -> FROM_PYTHON_MAP.put(v, k));
        }

        private SymbolHelper() {
            // static methods only, do not instantiate
        }

        public static String toPython(final String symbol) {
            return TO_PYTHON_MAP.getOrDefault(symbol, symbol);
        }

        public static String toJava(final String symbol) {
            return FROM_PYTHON_MAP.getOrDefault(symbol, symbol);
        }

    }
}