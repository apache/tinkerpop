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

package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.NumberHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.STRATEGY;
import static org.apache.tinkerpop.gremlin.util.DatetimeHelper.format;

/**
 *
 */
public class GremlinLang implements Cloneable, Serializable {

    private static final Object[] EMPTY_ARRAY = new Object[]{};

    private List<Instruction> sourceInstructions = new ArrayList<>();
    private List<Instruction> stepInstructions = new ArrayList<>();

    private StringBuilder gremlin = new StringBuilder();
    private Map<String, Object> parameters = new HashMap<>();
    private static final AtomicInteger paramCount = new AtomicInteger(0);
    // [Discuss] probably ThreadLocal<Integer> is faster, but unsafe for multithreaded traversal construction.
    // private static final ThreadLocal<Integer> paramCount = ThreadLocal.withInitial(() -> 0);
    private final List<OptionsStrategy> optionsStrategies = new ArrayList<>();

    public GremlinLang() {
    }

    public GremlinLang(final String sourceName, final Object... arguments) {
        this.sourceInstructions.add(new Instruction(sourceName, flattenArguments(arguments)));
        addToGremlin(sourceName, arguments);
    }

    private void addToGremlin(final String name, final Object... arguments) {
        final Object[] flattenedArguments = flattenArguments(arguments);

        // todo: figure out solution for AbstractLambdaTraversal
        if (name.equals("CardinalityValueTraversal")) {
            gremlin.append("Cardinality.").append(flattenedArguments[0])
                    .append("(").append(flattenedArguments[1]).append(")");
            return;
        }

        gremlin.append(".").append(name).append('(');

        for (int i = 0; i < flattenedArguments.length; i++) {
            if (i != 0) {
                gremlin.append(',');
            }
            gremlin.append(argAsString(flattenedArguments[i]));
        }

        gremlin.append(')');
    }

    private String argAsString(final Object arg) {
        if (arg == null)
            return "null";

        if (arg instanceof String)
            return String.format("\"%s\"", StringEscapeUtils.escapeJava((String) arg));
        if (arg instanceof Boolean)
            return arg.toString();

        if (arg instanceof Byte)
            return String.format("%sB", arg);
        if (arg instanceof Short)
            return String.format("%sS", arg);
        if (arg instanceof Integer)
            return arg.toString();
        if (arg instanceof Long)
            return String.format("%sL", arg);
        if (arg instanceof Float) {
            if (NumberHelper.isNaN(arg))
                return "NaN";
            if (NumberHelper.isPositiveInfinity(arg))
                return "+Infinity";
            if (NumberHelper.isNegativeInfinity(arg))
                return "-Infinity";

            return String.format("%sF", arg);
        }
        if (arg instanceof Double) {
            if (NumberHelper.isNaN(arg))
                return "NaN";
            if (NumberHelper.isPositiveInfinity(arg))
                return "+Infinity";
            if (NumberHelper.isNegativeInfinity(arg))
                return "-Infinity";
            return String.format("%sD", arg);
        }

        if (arg instanceof Date)
            return String.format("datetime(\"%s\")", format(((Date) arg).toInstant()));

        if (arg instanceof Enum) {
            // special handling for enums with additional interfaces
            if (arg instanceof T)
                return String.format("T.%s", arg);
            if (arg instanceof Order)
                return String.format("Order.%s", arg);
            if (arg instanceof Column)
                return String.format("Column.%s", arg);
            if (arg instanceof Operator)
                return String.format("Operator.%s", arg);
            if (arg instanceof SackFunctions.Barrier)
                return String.format("Barrier.%s", arg);

            return String.format("%s.%s", arg.getClass().getSimpleName(), arg);
        }

        if (arg instanceof Vertex)
            return String.format("new ReferenceVertex(%s,\"%s\")", argAsString(((Vertex) arg).id()), ((Vertex) arg).label());

        if (arg instanceof P) {
            return predicateAsString((P<?>) arg);
        }

        if (arg instanceof GremlinLang || arg instanceof DefaultTraversal) {
            final GremlinLang gremlinLang = arg instanceof GremlinLang ? (GremlinLang) arg : ((DefaultTraversal) arg).getGremlinLang();

            parameters.putAll(gremlinLang.getParameters());
            return gremlinLang.getGremlin("__");
        }

        // special handling for MergeV when map argument can have cardinality traversal inside
        if (arg instanceof Map) {
            final Map<Object, Object> asMap = (Map) arg;
            final AtomicBoolean containsTraversalValue = new AtomicBoolean(false);
            asMap.forEach((key, value) -> {
                if (value instanceof GremlinLang || value instanceof DefaultTraversal) {
                    containsTraversalValue.set(true);
                }
            });

            if (containsTraversalValue.get()) {
                return mapAsString(asMap);
            }
        }

        //final String paramName = String.format("_%d", paramCount.get());
        //paramCount.set(paramCount.get() + 1);
        final String paramName = String.format("_%d", paramCount.getAndIncrement());
        // todo: consider resetting paramCount when it's larger then 1_000_000
        parameters.put(paramName, arg);
        return paramName;
    }

    // borrowed from Groovy translator
    private String predicateAsString(final P<?> p) {
        final StringBuilder sb = new StringBuilder();
        if (p instanceof TextP) {
            sb.append("TextP.").append(p.getPredicateName()).append("(");
            sb.append(argAsString(p.getValue()));
        } else if (p instanceof ConnectiveP) {
            // ConnectiveP gets some special handling because it's reduced to and(P, P, P) and we want it
            // generated the way it was written which was P.and(P).and(P)
            final List<P<?>> list = ((ConnectiveP) p).getPredicates();
            final String connector = p.getPredicateName();
            for (int i = 0; i < list.size(); i++) {
                sb.append(argAsString(list.get(i)));

                // for the first/last P there is no parent to close
                if (i > 0 && i < list.size() - 1) sb.append(")");

                // add teh connector for all but last P
                if (i < list.size() - 1) {
                    sb.append(".").append(connector).append("(");
                }
            }
        } else {
            sb.append("P.").append(p.getPredicateName()).append("(");
            sb.append(argAsString(p.getValue()));
        }
        sb.append(")");
        return sb.toString();
    }

    final String mapAsString(final Map<?, ?> map) {
        final StringBuilder sb = new StringBuilder("[");
        int size = map.size();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            sb.append(argAsString(entry.getKey())).append(":").append(argAsString(entry.getValue()));
            if (--size > 0) {
                sb.append(',');
            }
        }

        sb.append("]");
        return sb.toString();
    }

    public String getGremlin() {
        return getGremlin("g");
    }

    // g for gts, __ for anonymous, any for gremlin-groovy
    private String getGremlin(final String g) {
        // special handling for CardinalityValueTraversal
        if (gremlin.length() != 0 && gremlin.charAt(0) != '.') {
            return gremlin.toString();
        }
        return g + gremlin;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void addG(final String g) {
        parameters.put("g", g);
    }

    /**
     * Add a {@link TraversalSource} instruction to the GremlinLang.
     *
     * @param sourceName the traversal source method name (e.g. withSack())
     * @param arguments  the traversal source method arguments
     */
    public void addSource(final String sourceName, final Object... arguments) {
        if (sourceName.equals(TraversalSource.Symbols.withoutStrategies)) {
            if (arguments == null)
                this.sourceInstructions.add(new Instruction(sourceName, null));
            else {
                final Class<TraversalStrategy>[] classes = new Class[arguments.length];
                for (int i = 0; i < arguments.length; i++) {
                    classes[i] = arguments[i] instanceof TraversalStrategyProxy ?
                            ((TraversalStrategyProxy) arguments[i]).getStrategyClass() :
                            (Class) arguments[i];
                }
                this.sourceInstructions.add(new Instruction(sourceName, classes));
            }
        } else
            this.sourceInstructions.add(new Instruction(sourceName, flattenArguments(arguments)));
        Bindings.clear();

        if (sourceName.equals(TraversalSource.Symbols.withStrategies) && arguments.length != 0) {
            final String args =  buildStrategyArgs(arguments);

            // possible to have empty strategies list to send
            if (!args.isEmpty()) {
                gremlin.append('.').append(TraversalSource.Symbols.withStrategies).append('(').append(args).append(')');
            }
            return;
        }

        addToGremlin(sourceName, arguments);
    }

    private String buildStrategyArgs(final Object[] arguments) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < arguments.length; i++) {
            // special handling for OptionsStrategy
            if (arguments[i] instanceof OptionsStrategy) {
                optionsStrategies.add((OptionsStrategy) arguments[i]);
                break;
            }

            final Configuration configuration = ((TraversalStrategy) arguments[i]).getConfiguration();

            if (configuration.isEmpty()) {
                sb.append(arguments[i].getClass().getSimpleName());
            } else {
                sb.append("new ")
                        .append(arguments[i].getClass().getSimpleName())
                        .append("(");

                configuration.getKeys().forEachRemaining(key -> {
                    if (!key.equals(STRATEGY)) {
                        sb.append(key).append(":").append(argAsString(configuration.getProperty(key))).append(",");
                    }
                });
                // remove last comma
                if (sb.lastIndexOf(",") == sb.length() - 1) {
                    sb.setLength(sb.length() - 1);
                }

                sb.append(')');
            }

            if (i != arguments.length - 1)
                sb.append(',');
        }

        return sb.toString();
    }

    /**
     * Add a {@link Traversal} instruction to the GremlinLang.
     *
     * @param stepName  the traversal method name (e.g. out())
     * @param arguments the traversal method arguments
     */
    public void addStep(final String stepName, final Object... arguments) {
        this.stepInstructions.add(new Instruction(stepName, flattenArguments(arguments)));

        addToGremlin(stepName, arguments);
    }

    public List<OptionsStrategy> getOptionsStrategies() {
        return optionsStrategies;
    }

    /**
     * Get the {@link TraversalSource} instructions associated with this bytecode.
     *
     * @return an iterable of instructions
     */
    public List<Instruction> getSourceInstructions() {
        return this.sourceInstructions;
    }

    /**
     * Get the {@link Traversal} instructions associated with this bytecode.
     *
     * @return an iterable of instructions
     */
    public List<Instruction> getStepInstructions() {
        return this.stepInstructions;
    }

    /**
     * Get both the {@link TraversalSource} and {@link Traversal} instructions of this bytecode.
     * The traversal source instructions are provided prior to the traversal instructions.
     *
     * @return an interable of all the instructions in this bytecode
     */
    public Iterable<Instruction> getInstructions() {
        return () -> IteratorUtils.concat(this.sourceInstructions.iterator(), this.stepInstructions.iterator());
    }

    /**
     * Get all the bindings (in a nested, recursive manner) from all the arguments of all the instructions of this bytecode.
     *
     * @return a map of string variable and object value bindings
     */
    public Map<String, Object> getBindings() {
        final Map<String, Object> bindingsMap = new HashMap<>();
        for (final Instruction instruction : this.sourceInstructions) {
            for (final Object argument : instruction.getArguments()) {
                addArgumentBinding(bindingsMap, argument);
            }
        }
        for (final Instruction instruction : this.stepInstructions) {
            for (final Object argument : instruction.getArguments()) {
                addArgumentBinding(bindingsMap, argument);
            }
        }
        return bindingsMap;
    }

    public boolean isEmpty() {
        return this.sourceInstructions.isEmpty() && this.stepInstructions.isEmpty();
    }

    private static void addArgumentBinding(final Map<String, Object> bindingsMap, final Object argument) {
        if (argument instanceof Binding)
            bindingsMap.put(((Binding) argument).key, ((Binding) argument).value);
        else if (argument instanceof Map) {
            for (final Map.Entry<?, ?> entry : ((Map<?, ?>) argument).entrySet()) {
                addArgumentBinding(bindingsMap, entry.getKey());
                addArgumentBinding(bindingsMap, entry.getValue());
            }
        } else if (argument instanceof Collection) {
            for (final Object item : (Collection) argument) {
                addArgumentBinding(bindingsMap, item);
            }
        } else if (argument instanceof GremlinLang)
            bindingsMap.putAll(((GremlinLang) argument).getBindings());
    }

    @Override
    public String toString() {
        return Arrays.asList(this.sourceInstructions, this.stepInstructions).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GremlinLang bytecode = (GremlinLang) o;
        return Objects.equals(sourceInstructions, bytecode.sourceInstructions) &&
                Objects.equals(stepInstructions, bytecode.stepInstructions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceInstructions, stepInstructions);
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public GremlinLang clone() {
        try {
            final GremlinLang clone = (GremlinLang) super.clone();
            clone.sourceInstructions = new ArrayList<>(this.sourceInstructions);
            clone.stepInstructions = new ArrayList<>(this.stepInstructions);

            clone.parameters = new HashMap<>(parameters);
            clone.gremlin = new StringBuilder(gremlin.length());
            clone.gremlin.append(gremlin);
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public static class Instruction implements Serializable {

        private final String operator;
        private final Object[] arguments;

        private Instruction(final String operator, final Object... arguments) {
            this.operator = operator;
            this.arguments = arguments;
        }

        public String getOperator() {
            return this.operator;
        }

        public Object[] getArguments() {
            return this.arguments;
        }

        @Override
        public String toString() {
            return this.operator + "(" + StringFactory.removeEndBrackets(Arrays.asList(this.arguments)) + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Instruction that = (Instruction) o;
            return Objects.equals(operator, that.operator) &&
                    Arrays.equals(arguments, that.arguments);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(operator);
            result = 31 * result + Arrays.hashCode(arguments);
            return result;
        }
    }

    public static class Binding<V> implements Serializable {

        private final String key;
        private final V value;

        public Binding(final String key, final V value) {
            this.key = key;
            this.value = value;
        }

        public String variable() {
            return this.key;
        }

        public V value() {
            return this.value;
        }

        @Override
        public String toString() {
            return "binding[" + this.key + "=" + this.value + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Binding<?> binding = (Binding<?>) o;
            return Objects.equals(key, binding.key) &&
                    Objects.equals(value, binding.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    /////

    private final Object[] flattenArguments(final Object... arguments) {
        if (arguments == null || arguments.length == 0)
            return EMPTY_ARRAY;
        final List<Object> flatArguments = new ArrayList<>(arguments.length);
        for (final Object object : arguments) {
            if (object instanceof Object[]) {
                for (final Object nestObject : (Object[]) object) {
                    flatArguments.add(convertArgument(nestObject, true));
                }
            } else
                flatArguments.add(convertArgument(object, true));
        }
        return flatArguments.toArray();
    }

    private final Object convertArgument(final Object argument, final boolean searchBindings) {
        if (searchBindings) {
            final String variable = Bindings.getBoundVariable(argument);
            if (null != variable)
                return new Binding<>(variable, convertArgument(argument, false));
        }
        //
        if (argument instanceof Traversal) {
            // prevent use of "g" to spawn child traversals
            if (((Traversal) argument).asAdmin().getTraversalSource().isPresent())
                throw new IllegalStateException(String.format(
                        "The child traversal of %s was not spawned anonymously - use the __ class rather than a TraversalSource to construct the child traversal", argument));

            return ((Traversal) argument).asAdmin().getGremlinLang();
        } else if (argument instanceof Map) {
            final Map<Object, Object> map = new LinkedHashMap<>(((Map) argument).size());
            for (final Map.Entry<?, ?> entry : ((Map<?, ?>) argument).entrySet()) {
                map.put(convertArgument(entry.getKey(), true), convertArgument(entry.getValue(), true));
            }
            return map;
        } else if (argument instanceof List) {
            final List<Object> list = new ArrayList<>(((List) argument).size());
            for (final Object item : (List) argument) {
                list.add(convertArgument(item, true));
            }
            return list;
        } else if (argument instanceof Set) {
            final Set<Object> set = new LinkedHashSet<>(((Set) argument).size());
            for (final Object item : (Set) argument) {
                set.add(convertArgument(item, true));
            }
            return set;
        } else
            return argument;
    }

    /////

}
