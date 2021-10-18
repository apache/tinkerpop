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

import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Script;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Converts bytecode to a Javascript string of Gremlin.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class JavascriptTranslator implements Translator.ScriptTranslator {

    private final String traversalSource;
    private final TypeTranslator typeTranslator;

    private JavascriptTranslator(final String traversalSource, final TypeTranslator typeTranslator) {
        this.traversalSource = traversalSource;
        this.typeTranslator = typeTranslator;
    }

    /**
     * Creates the translator with a {@code false} argument to {@code withParameters} using
     * {@link #of(String, boolean)}.
     */
    public static JavascriptTranslator of(final String traversalSource) {
        return of(traversalSource, false);
    }

    /**
     * Creates the translator with the {@link DefaultTypeTranslator} passing the {@code withParameters} option to it
     * which will handle type translation in a fashion that should typically increase cache hits and reduce
     * compilation times if enabled at the sacrifice to rewriting of the script that could reduce readability.
     */
    public static JavascriptTranslator of(final String traversalSource, final boolean withParameters) {
        return of(traversalSource, new DefaultTypeTranslator(withParameters));
    }

    /**
     * Creates the translator with a custom {@link TypeTranslator} instance.
     */
    public static JavascriptTranslator of(final String traversalSource, final TypeTranslator typeTranslator) {
        return new JavascriptTranslator(traversalSource, typeTranslator);
    }

    @Override
    public Script translate(final Bytecode bytecode) {
        return typeTranslator.apply(traversalSource, bytecode);
    }

    @Override
    public String getTargetLanguage() {
        return "gremlin-javascript";
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
     * Performs standard type translation for the TinkerPop types to Javascript.
     */
    public static class DefaultTypeTranslator extends AbstractTypeTranslator {

        public DefaultTypeTranslator(final boolean withParameters) {
            super(withParameters);
        }

        @Override
        protected String getNullSyntax() {
            return "null";
        }

        @Override
        protected String getSyntax(final String o) {
            return (o.contains("\"") ? "\"\"\"" + StringEscapeUtils.escapeJava(o) + "\"\"\"" : "\"" + StringEscapeUtils.escapeJava(o) + "\"")
                    .replace("$", "\\$");
        }

        @Override
        protected String getSyntax(final Boolean o) {
            return o.toString();
        }

        @Override
        protected String getSyntax(final Date o) {
            return "new Date(" + o.getTime() + ")";
        }

        @Override
        protected String getSyntax(final Timestamp o) {
            return "new Date(" + o.getTime() + ")";
        }

        @Override
        protected String getSyntax(final UUID o) {
            return "'" + o.toString() + "'";
        }

        @Override
        protected String getSyntax(final Lambda o) {
            return "() => \"" + StringEscapeUtils.escapeEcmaScript(o.getLambdaScript().trim()) + "\"";
        }

        @Override
        protected String getSyntax(final SackFunctions.Barrier o) {
            return "Barrier." + o.toString();
        }

        @Override
        protected String getSyntax(final VertexProperty.Cardinality o) {
            return "Cardinality." + o.toString();
        }

        @Override
        protected String getSyntax(final TraversalOptionParent.Pick o) {
            return "Pick." + o.toString();
        }

        @Override
        protected String getSyntax(final Number o) {
            return o.toString();
        }

        @Override
        protected Script produceScript(final Set<?> o) {
            return produceScript(new ArrayList<>(o));
        }

        @Override
        protected Script produceScript(final List<?> o) {
            final Iterator<?> iterator = ((List<?>) o).iterator();
            script.append("[");

            while (iterator.hasNext()) {
                final Object nextItem = iterator.next();
                convertToScript(nextItem);
                if (iterator.hasNext())
                    script.append(",").append(" ");
            }

            return script.append("]");
        }

        @Override
        protected Script produceScript(final Map<?, ?> o) {
            script.append("new Map([");
            final Iterator<? extends Map.Entry<?, ?>> itty = ((Map<?, ?>) o).entrySet().iterator();
            while (itty.hasNext()) {
                final Map.Entry<?,?> entry = itty.next();
                script.append("[");
                convertToScript(entry.getKey());
                script.append(",");
                convertToScript(entry.getValue());
                script.append("]");
                if (itty.hasNext())
                    script.append(",");
            }
            return script.append("])");
        }

        @Override
        protected Script produceScript(final Class<?> o) {
            return script.append(o.getCanonicalName());
        }

        @Override
        protected Script produceScript(final Enum<?> o) {
            return script.append(o.getDeclaringClass().getSimpleName() + "." + o.toString());
        }

        @Override
        protected Script produceScript(final Vertex o) {
            script.append("new Vertex(");
            convertToScript(o.id());
            script.append(",");
            convertToScript(o.label());
            return script.append(", null)");
        }

        @Override
        protected Script produceScript(final Edge o) {
            script.append("new Edge(");
            convertToScript(o.id());
            script.append(", new Vertex(");
            convertToScript(o.outVertex().id());
            script.append(",");
            convertToScript(o.outVertex().label());
            script.append(", null),");
            convertToScript(o.label());
            script.append(", new Vertex(");
            convertToScript(o.inVertex().id());
            script.append(",");
            convertToScript(o.inVertex().label());
            return script.append(",null),null)");
        }

        @Override
        protected Script produceScript(final VertexProperty<?> o) {
            script.append("new Property(");
            convertToScript(o.id());
            script.append(",");
            convertToScript(o.label());
            script.append(",");
            convertToScript(o.value());
            script.append(",");
            return script.append("null)");
        }

        @Override
        protected Script produceScript(final TraversalStrategyProxy<?> o) {
            if (o.getConfiguration().isEmpty()) {
                return script.append("new " + o.getStrategyClass().getSimpleName() + "()");
            } else {
                script.append("new " + o.getStrategyClass().getSimpleName() + "(");
                final Map<Object,Object> conf = ConfigurationConverter.getMap(o.getConfiguration());
                script.append("{");
                conf.entrySet().forEach(entry -> {
                    script.append(entry.getKey().toString());
                    script.append(":");
                    convertToScript(entry.getValue()).getScript();
                    script.append(",");
                });
                script.setCharAtEnd('}');
                return script.append(")");
            }
        }

        @Override
        protected Script produceScript(final String traversalSource, final Bytecode o) {
            script.append(traversalSource);
            for (final Bytecode.Instruction instruction : o.getInstructions()) {
                final String methodName = instruction.getOperator();
                if (0 == instruction.getArguments().length) {
                    script.append(".").append(resolveSymbol(methodName)).append("()");
                } else {
                    script.append(".").append(resolveSymbol(methodName)).append("(");

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

        @Override
        protected Script produceScript(final P<?> p) {
            if (p instanceof TextP) {
                script.append("TextP.").append(p.getBiPredicate().toString()).append("(");
                convertToScript(p.getValue());
            } else if (p instanceof ConnectiveP) {
                // ConnectiveP gets some special handling because it's reduced to and(P, P, P) and we want it
                // generated the way it was written which was P.and(P).and(P)
                final List<P<?>> list = ((ConnectiveP) p).getPredicates();
                final String connector = p instanceof OrP ? "or" : "and";
                for (int i = 0; i < list.size(); i++) {
                    produceScript(list.get(i));

                    // for the first/last P there is no parent to close
                    if (i > 0 && i < list.size() - 1) script.append(")");

                    // add teh connector for all but last P
                    if (i < list.size() - 1) {
                        script.append(".").append(connector).append("(");
                    }
                }
            } else {
                script.append("P.").append(p.getBiPredicate().toString()).append("(");
                convertToScript(p.getValue());
            }
            script.append(")");
            return script;
        }

        protected String resolveSymbol(final String methodName) {
            return SymbolHelper.toJavascript(methodName);
        }
    }

    static final class SymbolHelper {

        private final static Map<String, String> TO_JS_MAP = new HashMap<>();
        private final static Map<String, String> FROM_JS_MAP = new HashMap<>();

        static {
            TO_JS_MAP.put("from", "from_");
            TO_JS_MAP.put("in", "in_");
            TO_JS_MAP.put("with", "with_");
            //
            TO_JS_MAP.forEach((k, v) -> FROM_JS_MAP.put(v, k));
        }

        private SymbolHelper() {
            // static methods only, do not instantiate
        }

        public static String toJavascript(final String symbol) {
            return TO_JS_MAP.getOrDefault(symbol, symbol);
        }

        public static String toJava(final String symbol) {
            return FROM_JS_MAP.getOrDefault(symbol, symbol);
        }

    }
}
