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

import org.apache.commons.text.StringEscapeUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Script;
import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.NumberHelper;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiPredicate;

/**
 * Converts bytecode to a C# string of Gremlin.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class DotNetTranslator implements Translator.ScriptTranslator {

    private final String traversalSource;
    private final TypeTranslator typeTranslator;

    private static final List<String> methodsWithArgsNotNeedingGeneric = Arrays.asList(GraphTraversal.Symbols.group,
            GraphTraversal.Symbols.groupCount, GraphTraversal.Symbols.sack);

    private DotNetTranslator(final String traversalSource, final TypeTranslator typeTranslator) {
        this.traversalSource = traversalSource;
        this.typeTranslator = typeTranslator;
    }

    /**
     * Creates the translator with a {@code false} argument to {@code withParameters} using
     * {@link #of(String, boolean)}.
     */
    public static DotNetTranslator of(final String traversalSource) {
        return of(traversalSource, false);
    }

    /**
     * Creates the translator with the {@link DefaultTypeTranslator} passing the {@code withParameters} option to it
     * which will handle type translation in a fashion that should typically increase cache hits and reduce
     * compilation times if enabled at the sacrifice to rewriting of the script that could reduce readability.
     */
    public static DotNetTranslator of(final String traversalSource, final boolean withParameters) {
        return of(traversalSource, new DefaultTypeTranslator(withParameters));
    }

    /**
     * Creates the translator with a custom {@link TypeTranslator} instance.
     */
    public static DotNetTranslator of(final String traversalSource, final TypeTranslator typeTranslator) {
        return new DotNetTranslator(traversalSource, typeTranslator);
    }

    @Override
    public Script translate(final Bytecode bytecode) {
        return typeTranslator.apply(traversalSource, bytecode);
    }

    @Override
    public String getTargetLanguage() {
        return "gremlin-dotnet";
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
     * Performs standard type translation for the TinkerPop types to C#.
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
            return "\"" + StringEscapeUtils.escapeJava(o) + "\"";
        }

        @Override
        protected String getSyntax(final Boolean o) {
            return o.toString();
        }

        @Override
        protected String getSyntax(final Date o) {
            return "DateTimeOffset.FromUnixTimeMilliseconds(" + o.getTime() + ")";
        }

        @Override
        protected String getSyntax(final Timestamp o) {
            return "DateTimeOffset.FromUnixTimeMilliseconds(" + o.getTime() + ")";
        }

        @Override
        protected String getSyntax(final UUID o) {
            return "new Guid(\"" + o.toString() + "\")";
        }

        @Override
        protected String getSyntax(final Lambda o) {
            return "Lambda.Groovy(\"" + StringEscapeUtils.escapeEcmaScript(o.getLambdaScript().trim()) + "\")";
        }

        @Override
        protected String getSyntax(final SackFunctions.Barrier o) {
            return "Barrier." + SymbolHelper.toCSharp(o.toString());
        }

        @Override
        protected String getSyntax(final VertexProperty.Cardinality o) {
            return "Cardinality." + SymbolHelper.toCSharp(o.toString());
        }

        @Override
        protected String getSyntax(final Pick o) {
            return "Pick." + SymbolHelper.toCSharp(o.toString());
        }

        @Override
        protected String getSyntax(final Number o) {
            if (o instanceof Float || o instanceof Double) {
                if (NumberHelper.isNaN(o))
                    return (o instanceof Float ? "Single" : "Double") + ".NaN";
                if (NumberHelper.isPositiveInfinity(o))
                    return (o instanceof Float ? "Single" : "Double") + ".PositiveInfinity";
                if (NumberHelper.isNegativeInfinity(o))
                    return (o instanceof Float ? "Single" : "Double") + ".NegativeInfinity";
            }
            return o.toString();
        }

        @Override
        protected Script produceScript(final Set<?> o) {
            final Iterator<?> iterator = o.iterator();
            script.append("new HashSet<object> {");

            while (iterator.hasNext()) {
                final Object nextItem = iterator.next();
                convertToScript(nextItem);
                if (iterator.hasNext())
                    script.append(", ");
            }

            return script.append("}");
        }

        @Override
        protected Script produceScript(final List<?> o) {
            final Iterator<?> iterator = ((List<?>) o).iterator();
            script.append("new List<object> {");

            while (iterator.hasNext()) {
                final Object nextItem = iterator.next();
                convertToScript(nextItem);
                if (iterator.hasNext())
                    script.append(", ");
            }

            return script.append("}");
        }

        @Override
        protected Script produceScript(final Map<?, ?> o) {
            script.append("new Dictionary<object,object> {");
            produceKeyValuesForMap(o);
            return script.append("}");
        }

        @Override
        protected Script produceScript(final Class<?> o) {
            return script.append(o.getCanonicalName());
        }

        @Override
        protected Script produceScript(final Enum<?> o) {
            final String e = o instanceof Direction ?
                    o.name().substring(0,1).toUpperCase() + o.name().substring(1).toLowerCase() :
                    o.name().substring(0,1).toUpperCase() + o.name().substring(1);
            return script.append(o.getDeclaringClass().getSimpleName() + "." + e);
        }

        @Override
        protected Script produceScript(final Vertex o) {
            script.append("new Vertex(");
            convertToScript(o.id());
            script.append(", ");
            convertToScript(o.label());
            return script.append(")");
        }

        @Override
        protected Script produceScript(final Edge o) {
            script.append("new Edge(");
            convertToScript(o.id());
            script.append(", new Vertex(");
            convertToScript(o.outVertex().id());
            script.append(", ");
            convertToScript(o.outVertex().label());
            script.append("), ");
            convertToScript(o.label());
            script.append(", new Vertex(");
            convertToScript(o.inVertex().id());
            script.append(", ");
            convertToScript(o.inVertex().label());
            return script.append("))");
        }

        @Override
        protected Script produceScript(final VertexProperty<?> o) {
            script.append("new VertexProperty(");
            convertToScript(o.id());
            script.append(", ");
            convertToScript(o.label());
            script.append(", ");
            convertToScript(o.value());
            script.append(", ");
            return script.append("null)");
        }

        @Override
        protected Script produceScript(final TraversalStrategyProxy<?> o) {
            if (o.getConfiguration().isEmpty()) {
                return script.append("new " + o.getStrategyClass().getSimpleName() + "()");
            } else {
                script.append("new " + o.getStrategyClass().getSimpleName() + "(");
                final Iterator<String> keys = IteratorUtils.stream(o.getConfiguration().getKeys()).
                        filter(e -> !e.equals(TraversalStrategy.STRATEGY)).iterator();
                while (keys.hasNext()) {
                    final String k = keys.next();
                    script.append(k);
                    script.append(": ");
                    convertToScript(o.getConfiguration().getProperty(k));
                    if (keys.hasNext())
                        script.append(", ");
                }

                return script.append(")");
            }
        }

        private Script produceKeyValuesForMap(final Map<?,?> m) {
            final Iterator<? extends Map.Entry<?, ?>> itty = m.entrySet().iterator();
            while (itty.hasNext()) {
                final Map.Entry<?,?> entry = itty.next();
                script.append("{");
                convertToScript(entry.getKey());
                script.append(", ");
                convertToScript(entry.getValue());
                script.append("}");
                if (itty.hasNext())
                    script.append(", ");
            }
            return script;
        }

        @Override
        protected Script produceScript(final String traversalSource, final Bytecode o) {
            script.append(traversalSource);
            int instructionPosition = 0;
            for (final Bytecode.Instruction instruction : o.getInstructions()) {
                final String methodName = instruction.getOperator();
                // perhaps too many if/then conditions for specifying generics. doesnt' seem like there is a clear
                // way to refactor this more nicely though.
                //
                // inject() only has types when called with it when its used as a start step
                if (0 == instruction.getArguments().length) {
                    if (methodName.equals(GraphTraversal.Symbols.fold) && o.getSourceInstructions().size() + o.getStepInstructions().size() > 1 ||
                            (methodName.equals(GraphTraversal.Symbols.inject) && instructionPosition > 0))
                        script.append(".").append(resolveSymbol(methodName).replace("<object>", "")).append("()");
                    else
                        script.append(".").append(resolveSymbol(methodName)).append("()");
                } else {
                    if (methodsWithArgsNotNeedingGeneric.contains(methodName) ||
                            (methodName.equals(GraphTraversal.Symbols.inject) && (Arrays.stream(instruction.getArguments()).noneMatch(Objects::isNull) || instructionPosition > 0)))
                        script.append(".").append(resolveSymbol(methodName).replace("<object>", "").replace("<object,object>", "")).append("(");
                    else
                        script.append(".").append(resolveSymbol(methodName)).append("(");

                    // have to special case withSack() because UnaryOperator and BinaryOperator signatures
                    // make it impossible for the interpreter to figure out which function to call. specifically we need
                    // to discern between:
                    //     withSack(A initialValue, UnaryOperator<A> splitOperator)
                    //     withSack(A initialValue, BinaryOperator<A> splitOperator)
                    // and:
                    //     withSack(Supplier<A> initialValue, UnaryOperator<A> mergeOperator)
                    //     withSack(Supplier<A> initialValue, BinaryOperator<A> mergeOperator)
                    if (methodName.equals(TraversalSource.Symbols.withSack) &&
                            instruction.getArguments().length == 2 && instruction.getArguments()[1] instanceof Lambda) {
                        final String castFirstArgTo = instruction.getArguments()[0] instanceof Lambda ? "ISupplier" : "";
                        final Lambda secondArg = (Lambda) instruction.getArguments()[1];
                        final String castSecondArgTo = secondArg.getLambdaArguments() == 1 ? "IUnaryOperator" : "IBinaryOperator";
                        if (!castFirstArgTo.isEmpty())
                            script.append(String.format("(%s) ", castFirstArgTo));
                        convertToScript(instruction.getArguments()[0]);
                        script.append(", (").append(castSecondArgTo).append(") ");
                        convertToScript(instruction.getArguments()[1]);
                        script.append(",");
                    } else if (methodName.equals(GraphTraversal.Symbols.mergeE) || methodName.equals(GraphTraversal.Symbols.mergeV)) {
                        // there must be at least one argument - if null go with Map
                        final Object instArg = instruction.getArguments()[0];
                        if (null == instArg) {
                            script.append("(IDictionary<object,object>) null");
                        } else {
                            if (instArg instanceof Traversal) {
                                script.append("(ITraversal) ");
                            } else {
                                script.append("(IDictionary<object,object>) ");
                            }
                            convertToScript(instArg);
                        }
                        script.append(",");
                    } else if (methodName.equals(GraphTraversal.Symbols.option) &&
                            instruction.getArguments().length == 2 && instruction.getArguments()[0] instanceof Merge) {
                        final Object[] instArgs = instruction.getArguments();
                        // trying to catch option(Merge,Traversal|Map)
                        convertToScript(instArgs[0]);
                        script.append(", ");
                        if (instArgs[1] instanceof Traversal || instArgs[1] instanceof Bytecode) {
                            script.append("(ITraversal) ");
                        } else {
                            script.append("(IDictionary<object,object>) ");
                        }
                        convertToScript(instArgs[1]);
                        script.append(",");
                    } else {
                        final Object[] instArgs = instruction.getArguments();
                        for (int idx = 0; idx < instArgs.length; idx++) {
                            final Object instArg = instArgs[idx];
                            // overloads might have trouble with null in calling the right one. add more as we find
                            // them i guess
                            if (null == instArg) {
                                if ((methodName.equals(GraphTraversal.Symbols.addV) && idx % 2 == 0) ||
                                     methodName.equals(GraphTraversal.Symbols.hasLabel)||
                                     methodName.equals(GraphTraversal.Symbols.hasKey)) {
                                    script.append("(string) ");
                                } else if (methodName.equals(GraphTraversal.Symbols.hasValue)) {
                                    script.append("(object) ");
                                } else if (methodName.equals(GraphTraversal.Symbols.has)) {
                                    if (instArgs.length == 2) {
                                        if ((instArgs[0] instanceof T || instArgs[0] instanceof String) && idx == 1) {
                                            script.append("(object) ");
                                        }
                                    } else if (instArgs.length == 3) {
                                        if ((instArgs[0] instanceof T || instArgs[0] instanceof String) && idx == 1) {
                                            script.append("(string) ");
                                        } else if ((instArgs[0] instanceof T || instArgs[0] instanceof String) && idx == 2) {
                                            script.append("(object) ");
                                        }
                                    }
                                }
                            }
                            convertToScript(instArg);
                            script.append(",");
                        }
                    }
                    script.setCharAtEnd(')');
                }
                instructionPosition++;
            }
            return script;
        }

        @Override
        protected Script produceScript(final P<?> p) {
            if (p instanceof TextP) {
                // special case the RegexPredicate since it isn't an enum. toString() for the final default will
                // typically cover implementations (generally worked for Text prior to 3.6.0)
                final BiPredicate<?, ?> tp = p.getBiPredicate();
                if (tp instanceof Text.RegexPredicate) {
                    final String regexToken = ((Text.RegexPredicate) p.getBiPredicate()).isNegate() ? "NotRegex" : "Regex";
                    script.append("TextP.").append(regexToken).append("(");
                } else if (tp instanceof Text) {
                    script.append("TextP.").append(SymbolHelper.toCSharp(((Text) p.getBiPredicate()).name())).append("(");
                } else {
                    script.append("TextP.").append(SymbolHelper.toCSharp(p.getBiPredicate().toString())).append("(");
                }
                convertToScript(p.getValue());
            } else if (p instanceof ConnectiveP) {
                // ConnectiveP gets some special handling because it's reduced to and(P, P, P) and we want it
                // generated the way it was written which was P.and(P).and(P)
                final List<P<?>> list = ((ConnectiveP) p).getPredicates();
                final String connector = p instanceof OrP ? "Or" : "And";
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
                script.append("P.").append(SymbolHelper.toCSharp(p.getBiPredicate().toString())).append("(");
                convertToScript(p.getValue());
            }
            script.append(")");
            return script;
        }

        protected String resolveSymbol(final String methodName) {
            return SymbolHelper.toCSharp(methodName);
        }
    }

    static final class SymbolHelper {

        private final static Map<String, String> TO_CS_MAP = new HashMap<>();
        private final static Map<String, String> FROM_CS_MAP = new HashMap<>();

        static {
            TO_CS_MAP.put(GraphTraversal.Symbols.branch, "Branch<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.cap, "Cap<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.choose, "Choose<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.coalesce, "Coalesce<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.constant, "Constant<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.elementMap, "ElementMap<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.flatMap, "FlatMap<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.fold, "Fold<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.group, "Group<object,object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.groupCount, "GroupCount<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.index, "Index<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.inject, "Inject<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.io, "Io<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.limit, "Limit<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.local, "Local<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.match, "Match<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.map, "Map<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.max, "Max<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.min, "Min<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.mean, "Mean<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.optional, "Optional<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.project, "Project<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.properties, "Properties<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.range, "Range<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.sack, "Sack<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.select, "Select<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.skip, "Skip<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.sum, "Sum<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.tail, "Tail<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.unfold, "Unfold<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.union, "Union<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.value, "Value<object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.valueMap, "ValueMap<object,object>");
            TO_CS_MAP.put(GraphTraversal.Symbols.values, "Values<object>");
            //
            TO_CS_MAP.forEach((k, v) -> FROM_CS_MAP.put(v, k));
        }

        private SymbolHelper() {
            // static methods only, do not instantiate
        }

        public static String toCSharp(final String symbol) {
            return TO_CS_MAP.getOrDefault(symbol, symbol.substring(0,1).toUpperCase() + symbol.substring(1));
        }

        public static String toJava(final String symbol) {
            return FROM_CS_MAP.getOrDefault(symbol, symbol.substring(0,1).toLowerCase() + symbol.substring(1));
        }

    }
}
