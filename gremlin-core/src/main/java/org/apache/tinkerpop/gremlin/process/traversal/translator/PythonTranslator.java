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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.NumberHelper;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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

    private PythonTranslator(final String traversalSource, final TypeTranslator typeTranslator) {
        this.traversalSource = traversalSource;
        this.typeTranslator = typeTranslator;
    }

    /**
     * Creates the translator with a {@code false} argument to {@code withParameters} using
     * {@link #of(String, boolean)}.
     */
    public static PythonTranslator of(final String traversalSource) {
        return of(traversalSource, false);
    }

    /**
     * Creates the translator with the {@link DefaultTypeTranslator} passing the {@code withParameters} option to it
     * which will handle type translation in a fashion that should typically increase cache hits and reduce
     * compilation times if enabled at the sacrifice to rewriting of the script that could reduce readability.
     */
    public static PythonTranslator of(final String traversalSource, final boolean withParameters) {
        return of(traversalSource, new DefaultTypeTranslator(withParameters));
    }

    /**
     * Creates the translator with a custom {@link TypeTranslator} instance.
     */
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
    public static class DefaultTypeTranslator extends AbstractTypeTranslator {

        public DefaultTypeTranslator(final boolean withParameters) {
            super(withParameters);
        }

        @Override
        protected String getNullSyntax() {
            return "None";
        }

        @Override
        protected String getSyntax(final String o) {
            return o.contains("'") || o.contains(System.lineSeparator()) ?
                    "\"\"\"" + o + "\"\"\"" : "'" + o + "'";
        }

        @Override
        protected String getSyntax(final Boolean o) {
            return o ? "True" : "False";
        }

        @Override
        protected String getSyntax(final Date o) {
            return "datetime.datetime.utcfromtimestamp(" + o.getTime() + " / 1000.0)";
        }

        @Override
        protected String getSyntax(final Timestamp o) {
            return "timestamp(" + o.getTime() + " / 1000.0)";
        }

        @Override
        protected String getSyntax(final UUID o) {
            return "UUID('" + o.toString() +"')";
        }

        @Override
        protected String getSyntax(final Lambda o) {
            final String lambdaString = o.getLambdaScript().trim();
            return "lambda: \"" + StringEscapeUtils.escapeJava(lambdaString) + "\"";
        }

        @Override
        protected String getSyntax(final Number o) {
            if (o instanceof Double || o instanceof Float || o instanceof BigDecimal) {
                if (NumberHelper.isNaN(o))
                    return "float('nan')";
                if (NumberHelper.isPositiveInfinity(o))
                    return "float('inf')";
                if (NumberHelper.isNegativeInfinity(o))
                    return "float('-inf')";

                return "float(" + o + ")";
            }
            if (o instanceof Byte)
                return "SingleByte(" + o + ")";

            // all int/short/BigInteger/long are just python int/bignum
            return o.toString();
        }

        @Override
        protected String getSyntax(final SackFunctions.Barrier o) {
            return "Barrier." + resolveSymbol(o.toString());
        }

        @Override
        protected String getSyntax(final VertexProperty.Cardinality o) {
            return "Cardinality." + resolveSymbol(o.toString());
        }

        @Override
        protected String getSyntax(final TraversalOptionParent.Pick o) {
            return "Pick." + resolveSymbol(o.toString());
        }

        @Override
        protected Script produceScript(final Set<?> o) {
            final Iterator<?> iterator = o.iterator();
            script.append("set(");
            while(iterator.hasNext()) {
                convertToScript(iterator.next());
                if (iterator.hasNext())
                    script.append(",");
            }
            return script.append(")");
        }

        @Override
        protected Script produceScript(final List<?> o) {
            final Iterator<?> iterator = o.iterator();
            script.append("[");
            while(iterator.hasNext()) {
                convertToScript(iterator.next());
                if (iterator.hasNext())
                    script.append(",");
            }
            return script.append("]");
        }

        @Override
        protected Script produceScript(final Map<?, ?> o) {
            script.append("{");
            final Iterator<? extends Map.Entry<?, ?>> itty = o.entrySet().iterator();
            while (itty.hasNext()) {
                final Map.Entry<?,?> entry = itty.next();
                convertToScript(entry.getKey()).append(":");
                convertToScript(entry.getValue());
                if (itty.hasNext())
                    script.append(",");
            }
            return script.append("}");
        }

        @Override
        protected Script produceScript(final Class<?> o) {
            return script.append("GremlinType(" + o.getCanonicalName() + ")");
        }

        @Override
        protected Script produceScript(final Enum<?> o) {
            return script.append(o.getDeclaringClass().getSimpleName() + "." + resolveSymbol(o.toString()));
        }

        @Override
        protected Script produceScript(final Vertex o) {
            script.append("Vertex(");
            convertToScript(o.id()).append(",");
            return convertToScript(o.label()).append(")");
        }

        @Override
        protected Script produceScript(final Edge o) {
            script.append("Edge(");
            convertToScript(o.id()).append(",");
            convertToScript(o.outVertex()).append(",");
            convertToScript(o.label()).append(",");
            return convertToScript(o.inVertex()).append(")");
        }

        @Override
        protected Script produceScript(final VertexProperty<?> o) {
            script.append("VertexProperty(");
            convertToScript(o.id()).append(",");
            convertToScript(o.label()).append(",");
            return convertToScript(o.value()).append(")");
        }

        @Override
        protected Script produceScript(final TraversalStrategyProxy<?> o) {
            if (o.getConfiguration().isEmpty())
                return script.append("TraversalStrategy('" + o.getStrategyClass().getSimpleName() + "', None, '" + o.getStrategyClass().getName() + "')");
            else {
                script.append("TraversalStrategy('").append(o.getStrategyClass().getSimpleName()).append("',");
                convertToScript(ConfigurationConverter.getMap(o.getConfiguration()));
                script.append(", '");
                script.append(o.getStrategyClass().getName());
                return script.append("')");
            }
        }

        @Override
        protected Script produceScript(final String traversalSource, final Bytecode o) {
            script.append(traversalSource);
            for (final Bytecode.Instruction instruction : o.getInstructions()) {
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
                    if (varargsBeware) script.append("*[");

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

        @Override
        protected Script produceScript(final P<?> p) {
            if (p instanceof TextP) {
                script.append("TextP.").append(resolveSymbol(p.getBiPredicate().toString())).append("(");
                convertToScript(p.getValue());
            } else if (p instanceof ConnectiveP) {
                // ConnectiveP gets some special handling because it's reduced to and(P, P, P) and we want it
                // generated the way it was written which was P.and(P).and(P)
                final List<P<?>> list = ((ConnectiveP) p).getPredicates();
                final String connector = p instanceof OrP ? "or_" : "and_";
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
                script.append("P.").append(resolveSymbol(p.getBiPredicate().toString())).append("(");
                convertToScript(p.getValue());
            }
            script.append(")");
            return script;
        }

        protected String resolveSymbol(final String methodName) {
            return SymbolHelper.toPython(methodName);
        }
    }

    /**
     * Performs translation without for the syntax sugar to Python.
     */
    public static class NoSugarTranslator extends DefaultTypeTranslator {

      public NoSugarTranslator(final boolean withParameters) {
            super(withParameters);
        }

        @Override
        protected Script produceScript(final String traversalSource, final Bytecode o) {
          script.append(traversalSource);
          for (final Bytecode.Instruction instruction : o.getInstructions()) {
            final String methodName = instruction.getOperator();
            final Object[] arguments = instruction.getArguments();
            if (0 == arguments.length)
              script.append(".").append(resolveSymbol(methodName)).append("()");
            else {
              script.append(".").append(resolveSymbol(methodName)).append("(");

              // python has trouble with java varargs...wrapping in collection seems to solve
              // the problem
              final boolean varargsBeware = instruction.getOperator().equals(TraversalSource.Symbols.withStrategies)
                  || instruction.getOperator().equals(TraversalSource.Symbols.withoutStrategies);
              if (varargsBeware)
                script.append("*[");

              final Iterator<?> itty = Stream.of(arguments).iterator();
              while (itty.hasNext()) {
                convertToScript(itty.next());
                if (itty.hasNext())
                  script.append(",");
              }

              if (varargsBeware)
                script.append("]");

              script.append(")");
            }
          }
          return script;
        }

    }
    static final class SymbolHelper {

        private final static Map<String, String> TO_PYTHON_MAP = new HashMap<>();
        private final static Map<String, String> FROM_PYTHON_MAP = new HashMap<>();

        static {
            TO_PYTHON_MAP.put("global", "global_");
            TO_PYTHON_MAP.put("all", "all_");
            TO_PYTHON_MAP.put("and", "and_");
            TO_PYTHON_MAP.put("as", "as_");
            TO_PYTHON_MAP.put("filter", "filter_");
            TO_PYTHON_MAP.put("from", "from_");
            TO_PYTHON_MAP.put("id", "id_");
            TO_PYTHON_MAP.put("in", "in_");
            TO_PYTHON_MAP.put("is", "is_");
            TO_PYTHON_MAP.put("list", "list_");
            TO_PYTHON_MAP.put("max", "max_");
            TO_PYTHON_MAP.put("min", "min_");
            TO_PYTHON_MAP.put("or", "or_");
            TO_PYTHON_MAP.put("not", "not_");
            TO_PYTHON_MAP.put("range", "range_");
            TO_PYTHON_MAP.put("set", "set_");
            TO_PYTHON_MAP.put("sum", "sum_");
            TO_PYTHON_MAP.put("with", "with_");
            TO_PYTHON_MAP.put("range", "range_");
            TO_PYTHON_MAP.put("filter", "filter_");
            TO_PYTHON_MAP.put("id", "id_");
            TO_PYTHON_MAP.put("max", "max_");
            TO_PYTHON_MAP.put("min", "min_");
            TO_PYTHON_MAP.put("sum", "sum_");
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