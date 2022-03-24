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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Translates Gremlin {@link Bytecode} into a Golang string representation.
 *
 * @author Simon Zhao (simonz@bitquilltech.com)
 */
public final class GolangTranslator implements Translator.ScriptTranslator {
    private final String traversalSource;
    private final TypeTranslator typeTranslator;
    private final static  String GO_PACKAGE_NAME = "gremlingo.";
    private final static Set<String> METHODS_TO_CHECK = new HashSet<>(Arrays.asList(
            "inject", "withSack", "sample", "with", "constant")
    );

    private GolangTranslator(final String traversalSource, final TypeTranslator typeTranslator) {
        this.traversalSource = traversalSource;
        this.typeTranslator = typeTranslator;
    }

    /**
     * Creates the translator with a {@code false} argument to {@code withParameters} using
     * {@link #of(String, boolean)}.
     */
    public static GolangTranslator of(final String traversalSource) {
        return of(traversalSource, false);
    }

    /**
     * Creates the translator with the {@link DefaultTypeTranslator} passing the {@code withParameters} option to it
     * which will handle type translation in a fashion that should typically increase cache hits and reduce
     * compilation times if enabled at the sacrifice to rewriting of the script that could reduce readability.
     */
    public static GolangTranslator of(final String traversalSource, final boolean withParameters) {
        return of(traversalSource, new DefaultTypeTranslator(withParameters));
    }

    /**
     * Creates the translator with a custom {@link TypeTranslator} instance.
     */
    public static GolangTranslator of(final String traversalSource, final TypeTranslator typeTranslator) {
        return new GolangTranslator(traversalSource, typeTranslator);
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
        return "gremlin-go";
    }

    @Override
    public String toString() {
        return StringFactory.translatorString(this);
    }

    ///////

    /**
     * Performs standard type translation for the TinkerPop types to Go.
     */
    public static class DefaultTypeTranslator extends AbstractTypeTranslator {

        public DefaultTypeTranslator(final boolean withParameters) {
            super(withParameters);
        }

        @Override
        protected String getNullSyntax() {
            return "nil";
        }

        @Override
        protected String getSyntax(final String o) {
            return "\"" + StringEscapeUtils.escapeJava(o) + "\"";
        }

        @Override
        protected String getSyntax(final Boolean o) {
            return o.toString();
        }
        @Override
        protected String getSyntax(final Date o) {
            return "time.Unix(" + o.getTime() + ", 0)";
        }

        @Override
        protected String getSyntax(final Timestamp o) {
            return "time.Unix(" + o.getTime() + ", 0)";
        }

        @Override
        protected String getSyntax(final UUID o) {
            return "uuid.MustParse(\"" + o.toString() + "\")";
        }

        @Override
        protected String getSyntax(final Lambda o) {
            return "&gremlingo.Lambda{Script:\"" + o.getLambdaScript().trim() + "\", Language:\"\"}";
        }

        @Override
        protected String getSyntax(final Number o) {
                return o.toString();
        }

        @Override
        protected String getSyntax(final SackFunctions.Barrier o) {
            return GO_PACKAGE_NAME + resolveSymbol(o.toString());
        }

        @Override
        protected String getSyntax(final VertexProperty.Cardinality o) {
            return GO_PACKAGE_NAME + resolveSymbol(o.toString());
        }

        @Override
        protected String getSyntax(final TraversalOptionParent.Pick o) {
            return GO_PACKAGE_NAME + resolveSymbol(o.toString());
        }

        @Override
        protected Script produceScript(final Set<?> o) {
            // TODO: AN-1044 Change this when Set type is added in Gremlin-Go
            final Iterator<?> iterator = o.iterator();
            script.append("[]interface{}{");
            while(iterator.hasNext()) {
                convertToScript(iterator.next());
                if (iterator.hasNext())
                    script.append(", ");
            }
            return script.append("}");
        }

        @Override
        protected Script produceScript(final List<?> o) {
            final Iterator<?> iterator = o.iterator();
            while(iterator.hasNext()) {
                convertToScript(iterator.next());
                if (iterator.hasNext())
                    script.append(", ");
            }
            return script;
        }

        @Override
        protected Script produceScript(final Map<?, ?> o) {
            script.append("map[interface{}]interface{}{");
            final Iterator<? extends Map.Entry<?, ?>> itty = o.entrySet().iterator();
            while (itty.hasNext()) {
                final Map.Entry<?,?> entry = itty.next();
                convertToScript(entry.getKey()).append(": ");
                convertToScript(entry.getValue());
                if (itty.hasNext())
                    script.append(", ");
            }
            return script.append("}");
        }

        @Override
        protected Script produceScript(final Class<?> o) {
            return script.append(o.getCanonicalName());
        }

        @Override
        protected Script produceScript(final Enum<?> o) {
            return script.append(GO_PACKAGE_NAME + resolveSymbol(o.toString()));
        }

        @Override
        protected Script produceScript(final Vertex o) {
            script.append(GO_PACKAGE_NAME + "Vertex{Element{");
            convertToScript(o.id()).append(", ");
            return convertToScript(o.label()).append("}}");
        }

        @Override
        protected Script produceScript(final Edge o) {
            script.append(GO_PACKAGE_NAME + "Edge{Element{");
            convertToScript(o.id()).append(", ");
            convertToScript(o.label()).append("}, ");
            convertToScript(o.outVertex()).append(",");
            return convertToScript(o.inVertex()).append("}");
        }

        @Override
        protected Script produceScript(final VertexProperty<?> o) {
            script.append(GO_PACKAGE_NAME + "VertexProperty{");
            convertToScript(o.id()).append(", ");
            convertToScript(o.label()).append("}, ");
            return convertToScript(o.value()).append("}");
        }

        @Override
        protected Script produceScript(final TraversalStrategyProxy<?> o) {
            if (o.getConfiguration().isEmpty()) {
                return script.append("strategyFactory(\"" + o.getStrategyClass().getSimpleName() + "\", map[string]interface{}{})");
            } else {
                script.append("strategyFactory(\"" + o.getStrategyClass().getSimpleName() + "\", map[string]interface{}{");
                final Iterator<String> keys = IteratorUtils.stream(o.getConfiguration().getKeys()).
                        filter(e -> !e.equals(TraversalStrategy.STRATEGY)).iterator();
                while (keys.hasNext()) {
                    final String k = keys.next();
                    script.append("\"");
                    script.append(k);
                    script.append("\": ");
                    convertToScript(o.getConfiguration().getProperty(k));
                    if (keys.hasNext())
                        script.append(", ");
                }

                return script.append("})");
            }
        }

        @Override
        protected Script produceScript(final String traversalSource, final Bytecode o) {
            // TODO: AN-1042 Ensure translation matches Gremlin-Go implementation when done
            final String source = traversalSource.equals("__") ? GO_PACKAGE_NAME + "T__" : traversalSource;
            script.append(source);
            for (final Bytecode.Instruction instruction : o.getInstructions()) {
                final String methodName = instruction.getOperator();
                final Object[] arguments = instruction.getArguments();

                script.append(".").append(resolveSymbol(methodName)).append("(");

                for (int i = 0; i < arguments.length; i++) {
                    if (methodName.equals("times")) {
                        script.append("int32(");
                        convertToScript(arguments[i]);
                        script.append(")");
                    } else if (METHODS_TO_CHECK.contains(methodName)
                            && arguments[i] instanceof Integer) {
                        script.append("int32(");
                        convertToScript(arguments[i]);
                        script.append(")");
                        if (i != arguments.length - 1) {
                            script.append(", ");
                        }
                    } else {
                        convertToScript(arguments[i]);
                        if (i != arguments.length - 1) {
                            script.append(", ");
                        }
                    }
                }

                script.append(")");
            }
            return script;
        }

        @Override
        protected Script produceScript(final P<?> p) {
            if (p instanceof TextP) {
                script.append(GO_PACKAGE_NAME + "TextP.").append(resolveSymbol(p.getBiPredicate().toString())).append("(");
                convertToScript(p.getValue());
            } else if (p instanceof ConnectiveP) {
                // ConnectiveP gets some special handling because it's reduced to and(P, P, P) and we want it
                // generated the way it was written which was P.and(P).and(P)
                final List<P<?>> list = ((ConnectiveP) p).getPredicates();
                final String connector = p instanceof OrP ? "Or" : "And";
                for (int i = 0; i < list.size(); i++) {
                    produceScript(list.get(i));

                    // For the first/last P there is no parent to close.
                    if (i > 0 && i < list.size() - 1) {
                        script.append(")");
                    }

                    // Add the connector for all but last P.
                    if (i < list.size() - 1) {
                        script.append(".").append(connector).append("(");
                    }
                }
            } else {
                script.append(GO_PACKAGE_NAME + "P.").append(resolveSymbol(p.getBiPredicate().toString())).append("(");
                convertToScript(p.getValue());
            }
            script.append(")");
            return script;
        }

        protected String resolveSymbol(final String methodName) {
            return SymbolHelper.toGolang(methodName);
        }
    }

    static final class SymbolHelper {

        private final static Map<String, String> TO_GO_MAP = new HashMap<>();
        private final static Map<String, String> FROM_GO_MAP = new HashMap<>();

        static {
            TO_GO_MAP.put("OUT", "Out");
            TO_GO_MAP.put("IN", "In");
            TO_GO_MAP.put("BOTH", "Both");
            TO_GO_MAP.put("set", "Set_");
            TO_GO_MAP.forEach((k, v) -> FROM_GO_MAP.put(v, k));
        }

        private SymbolHelper() {
            // static methods only, do not instantiate
        }

        public static String toGolang(final String symbol) {
            return TO_GO_MAP.getOrDefault(symbol, StringUtils.capitalize(symbol));
        }

        public static String toJava(final String symbol) {
            return FROM_GO_MAP.getOrDefault(symbol, StringUtils.uncapitalize(symbol));
        }

    }
}
