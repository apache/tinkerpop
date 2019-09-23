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

import groovy.json.StringEscapeUtils;
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

import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Converts bytecode to a Groovy string of Gremlin.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Stark Arya (sandszhou.zj@alibaba-inc.com)
 */
public final class GroovyTranslator implements Translator.ScriptTranslator {

    private final String traversalSource;
    private final TypeTranslator typeTranslator;

    private GroovyTranslator(final String traversalSource, final TypeTranslator typeTranslator) {
        this.traversalSource = traversalSource;
        this.typeTranslator = typeTranslator;
    }

    /**
     * Creates the translator with a {@code false} argument to {@code withParameters} using
     * {@link #of(String, boolean)}.
     */
    public static final GroovyTranslator of(final String traversalSource) {
        return of(traversalSource, false);
    }

    /**
     * Creates the translator with the {@link DefaultTypeTranslator} passing the {@code withParameters} option to it
     * which will handle type translation in a fashion that should typically increase cache hits and reduce
     * compilation times if enabled at the sacrifice to rewriting of the script that could reduce readability.
     */
    public static final GroovyTranslator of(final String traversalSource, final boolean withParameters) {
        return of(traversalSource, new DefaultTypeTranslator(withParameters));
    }

    public static final GroovyTranslator of(final String traversalSource, final TypeTranslator typeTranslator) {
        return new GroovyTranslator(traversalSource, typeTranslator);
    }

    ///////

    @Override
    public Script translate(final Bytecode bytecode) {
        return typeTranslator.apply(traversalSource, bytecode);
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
     * Performs standard type translation for the TinkerPop types to Groovy.
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
         *  For each operator argument, if withParameters set true, try parametrization as follows:
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
         *  (Yes)                 Other
         *  -------------------------------------------------
         * @param object
         * @return String Repres
         */
        protected Script convertToScript(final Object object) {
            if (object instanceof Bytecode.Binding) {
                return script.getBoundKeyOrAssign(withParameters, ((Bytecode.Binding) object).variable());
            } else if (object instanceof Bytecode) {
                return internalTranslate("__", (Bytecode) object);
            } else if (object instanceof Traversal) {
                return convertToScript(((Traversal) object).asAdmin().getBytecode());
            } else if (object instanceof String) {
                final String wrapper = (((String) object).contains("\"") ? "\"\"\"" + StringEscapeUtils.escapeJava((String) object) + "\"\"\"" : "\"" + StringEscapeUtils.escapeJava((String) object) + "\"")
                        .replace("$", "\\$");
                return script.getBoundKeyOrAssign(withParameters, withParameters ? object : wrapper);
            } else if (object instanceof Set) {
                convertToScript(((Set)object).stream().collect(Collectors.toList()));
                return script.append(" as Set");
            } else if (object instanceof List) {
                Iterator<?> iterator = ((List)object).iterator();
                if (! iterator.hasNext()) {
                    return script.append("[]");
                } else {
                    script.append("[");
                    for (;;)  {
                        Object e =  iterator.next();
                        convertToScript(e);
                        if (! iterator.hasNext()) {
                            return script.append("]");
                        } else {
                            script.append(",").append(" ");
                        }
                    }
                }
            } else if (object instanceof Map) {
                script.append("[");
                for (final Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                    script.append("(");
                    convertToScript(entry.getKey());
                    script.append("):(");
                    convertToScript(entry.getValue());
                    script.append("),");
                }

                // only need to remove this last bit if entries were added
                if (!((Map<?, ?>) object).isEmpty()) {
                    return script.setCharAtEnd(']');
                } else {
                    return script.append("]");
                }
            } else if (object instanceof Long) {
                return script.getBoundKeyOrAssign(withParameters, withParameters ? object : object + "L");
            } else if (object instanceof Double) {
                return script.getBoundKeyOrAssign(withParameters, withParameters ? object : object + "d");
            } else if (object instanceof Float) {
                return script.getBoundKeyOrAssign(withParameters, withParameters ? object : object + "f");
            } else if (object instanceof Integer) {
                return script.getBoundKeyOrAssign(withParameters, withParameters ? object : "(int) " + object);
            } else if (object instanceof Class) {
                return script.append(((Class) object).getCanonicalName());
            } else if (object instanceof Timestamp) {
                return script.getBoundKeyOrAssign(withParameters, withParameters ? object : "new java.sql.Timestamp(" + ((Timestamp) object).getTime() + ")");
            } else if (object instanceof Date) {
                return script.getBoundKeyOrAssign(withParameters, withParameters ? object : "new java.util.Date(" + ((Date) object).getTime() + ")");
            } else if (object instanceof UUID) {
                return script.getBoundKeyOrAssign(withParameters, withParameters ? object : "java.util.UUID.fromString('" + object.toString() + "')");
            } else if (object instanceof P) {
                return convertPToScript((P) object);
            } else if (object instanceof SackFunctions.Barrier) {
                return script.append("SackFunctions.Barrier." + object.toString());
            } else if (object instanceof VertexProperty.Cardinality) {
                return script.append("VertexProperty.Cardinality." + object.toString());
            } else if (object instanceof TraversalOptionParent.Pick) {
                return script.append("TraversalOptionParent.Pick." + object.toString());
            } else if (object instanceof Enum) {
                return script.append(((Enum) object).getDeclaringClass().getSimpleName() + "." + object.toString());
            } else if (object instanceof Element) {
                if (object instanceof Vertex) {
                    final Vertex vertex = (Vertex) object;
                    script.append("new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(");
                    convertToScript(vertex.id());
                    script.append(",");
                    convertToScript(vertex.label());
                    return script.append(", Collections.emptyMap())");
                } else if (object instanceof Edge) {
                    final Edge edge = (Edge) object;
                    script.append("new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge(");
                    convertToScript(edge.id());
                    script.append(",");
                    convertToScript(edge.label());
                    script.append(",");
                    script.append("Collections.emptyMap(),");
                    convertToScript(edge.outVertex().id());
                    script.append(",");
                    convertToScript(edge.outVertex().label());
                    script.append(",");
                    convertToScript(edge.inVertex().id());
                    script.append(",");
                    convertToScript(edge.inVertex().label());
                    return script.append(")");
                } else {// VertexProperty
                    final VertexProperty vertexProperty = (VertexProperty) object;
                    script.append("new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty(");
                    convertToScript(vertexProperty.id());
                    script.append(",");
                    convertToScript(vertexProperty.label());
                    script.append(",");
                    convertToScript(vertexProperty.value());
                    script.append(",");
                    script.append("Collections.emptyMap(),");
                    convertToScript(vertexProperty.element());
                    return script.append(")");
                }
            } else if (object instanceof Lambda) {
                final String lambdaString = ((Lambda) object).getLambdaScript().trim();
                final String wrapper = lambdaString.startsWith("{") ? lambdaString : "{" + lambdaString + "}";
                return script.getBoundKeyOrAssign(withParameters, withParameters ? object : wrapper);
            } else if (object instanceof TraversalStrategyProxy) {
                final TraversalStrategyProxy proxy = (TraversalStrategyProxy) object;
                if (proxy.getConfiguration().isEmpty()) {
                    return script.append(proxy.getStrategyClass().getCanonicalName() + ".instance()");
                } else {
                    script.append(proxy.getStrategyClass().getCanonicalName() + ".create(new org.apache.commons.configuration2.MapConfiguration(");
                    convertToScript(ConfigurationConverter.getMap(proxy.getConfiguration()));
                    return script.append("))");
                }
            } else if (object instanceof TraversalStrategy) {
                return convertToScript(new TraversalStrategyProxy(((TraversalStrategy) object)));
            } else {
                return null == object ? script.append("null") : script.getBoundKeyOrAssign(withParameters, object);
            }
        }

        protected Script internalTranslate(final String start, final Bytecode bytecode) {
            script.append(start);
            for (final Bytecode.Instruction instruction : bytecode.getInstructions()) {
                final String methodName = instruction.getOperator();
                if (0 == instruction.getArguments().length) {
                    script.append(".").append(methodName).append("()");
                } else {
                    script.append(".").append(methodName).append("(");

                    // have to special case withSack() for Groovy because UnaryOperator and BinaryOperator signatures
                    // make it impossible for the interpreter to figure out which function to call. specifically we need
                    // to discern between:
                    //     withSack(A initialValue, UnaryOperator<A> splitOperator)
                    //     withSack(A initialValue, BinaryOperator<A> splitOperator)
                    // and:
                    //     withSack(Supplier<A> initialValue, UnaryOperator<A> mergeOperator)
                    //     withSack(Supplier<A> initialValue, BinaryOperator<A> mergeOperator)
                    if (methodName.equals(TraversalSource.Symbols.withSack) &&
                            instruction.getArguments().length == 2 && instruction.getArguments()[1] instanceof Lambda) {
                        final String castFirstArgTo = instruction.getArguments()[0] instanceof Lambda ?
                                Supplier.class.getName() : "";
                        final Lambda secondArg = (Lambda) instruction.getArguments()[1];
                        final String castSecondArgTo = secondArg.getLambdaArguments() == 1 ? UnaryOperator.class.getName() :
                                BinaryOperator.class.getName();
                        if (!castFirstArgTo.isEmpty())
                            script.append(String.format("(%s) ", castFirstArgTo));
                        convertToScript(instruction.getArguments()[0]);
                        script.append(", (").append(castSecondArgTo).append(") ");
                        convertToScript(instruction.getArguments()[1]);
                        script.append(",");
                    } else {
                        for (final Object object : instruction.getArguments()) {
                            convertToScript(object);
                            script.append(",");
                        }
                    }
                    script.setCharAtEnd(')');
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
                        script.append(p instanceof OrP ? ".or(" : ".and(");
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
    }
}
