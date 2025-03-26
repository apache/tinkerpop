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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.NumberHelper;

import javax.lang.model.SourceVersion;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.STRATEGY;
import static org.apache.tinkerpop.gremlin.util.DatetimeHelper.format;
import static org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils.asIterator;

/**
 * This class helps to build a gremlin-lang compatible string representation based on a {@link TraversalSource}
 * and then a {@link Traversal}.
 */
public class GremlinLang implements Cloneable, Serializable {

    private static final Object[] EMPTY_ARRAY = new Object[]{};

    private StringBuilder gremlin = new StringBuilder();
    private Map<String, Object> parameters = new HashMap<>();
    private static final AtomicInteger paramCount = new AtomicInteger(0);
    private List<OptionsStrategy> optionsStrategies = new ArrayList<>();

    public GremlinLang() {
    }

    public GremlinLang(final String sourceName, final Object... arguments) {
        addToGremlin(sourceName, arguments);
    }

    private void addToGremlin(final String name, final Object... arguments) {
        final Object[] flattenedArguments = flattenArguments(arguments);

        // todo: figure out solution for AbstractLambdaTraversal
        if ("CardinalityValueTraversal".equals(name)) {
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

        if (arg instanceof BigInteger)
            return String.format("%sN", arg);
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
        if (arg instanceof BigDecimal)
            return String.format("%sM", arg);

        if (arg instanceof OffsetDateTime)
            return String.format("datetime(\"%s\")", format(((OffsetDateTime) arg).toInstant()));

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
            return asString((P<?>) arg);
        }

        if (arg instanceof GremlinLang || arg instanceof DefaultTraversal) {
            final GremlinLang gremlinLang = arg instanceof GremlinLang ? (GremlinLang) arg : ((DefaultTraversal) arg).getGremlinLang();
            parameters.putAll(gremlinLang.getParameters());
            return gremlinLang.getGremlin("__");
        }

        if (arg instanceof GValue) {
            final GValue gValue = (GValue) arg;
            String key = gValue.getName();

            if (key == null) {
                return argAsString(((GValue<?>) arg).get());
            }

            if (!SourceVersion.isIdentifier(key)) {
                throw new IllegalArgumentException(String.format("Invalid parameter name [%s].", key));
            }

            if (parameters.containsKey(key)) {
                if (!Objects.equals(parameters.get(key), gValue.get())) {
                    throw new IllegalArgumentException(String.format("Parameter with name [%s] already defined.", key));
                }
            } else {
                parameters.put(key, gValue.get());
            }
            return key;
        }

        if (arg instanceof Map) {
            return asString((Map) arg);
        }

        if (arg instanceof Set) {
            return asString((Set) arg);
        }

        // handle all iterables in similar way
        if (arg instanceof List || arg instanceof Object[] || arg.getClass().isArray()) {
            return asString(asIterator(arg));
        }

        if (arg instanceof Class) {
            return ((Class) arg).getSimpleName();
        }

        return asParameter(arg);
    }

    private String asParameter(final Object arg) {
        final String paramName = String.format("_%d", paramCount.getAndIncrement());
        // todo: consider resetting paramCount when it's larger then 1_000_000
        parameters.put(paramName, arg);
        return paramName;
    }

    private String asString(final Iterator itty) {
        final StringBuilder sb = new StringBuilder().append("[");

        while (itty.hasNext()) {
            sb.append(argAsString(itty.next()));
            if (itty.hasNext())
                sb.append(",");
        }

        return sb.append("]").toString();
    }

    private String asString(final Set<?> set) {
        final StringBuilder sb = new StringBuilder().append("{");

        final Iterator itty = asIterator(set);

        while (itty.hasNext()) {
            sb.append(argAsString(itty.next()));
            if (itty.hasNext())
                sb.append(",");
        }

        return sb.append("}").toString();
    }

    // borrowed from Groovy translator
    private String asString(final P<?> p) {
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

    private String asString(final Map<?, ?> map) {
        final StringBuilder sb = new StringBuilder("[");
        int size = map.size();

        if (size == 0) {
            sb.append(":");
        } else {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = argAsString(entry.getKey());
                // special handling for non-string keys
                if (entry.getKey() instanceof Enum && key.contains(".")) {
                    key = String.format("(%s)", key);
                }

                sb.append(key).append(":").append(argAsString(entry.getValue()));
                if (--size > 0) {
                    sb.append(',');
                }
            }
        }

        sb.append("]");
        return sb.toString();
    }

    /**
     * Get gremlin-lang compatible representation of Traversal
     * @return gremlin-lang compatible String
     */
    public String getGremlin() {
        return getGremlin("g");
    }

    /**
     * Get gremlin-lang compatible representation of Traversal.
     * "g" is expected for gremlin-lang.
     * "__" can be used for an anonymous {@link GraphTraversal}.
     *
     * @param g GraphTraversalSource name
     * @return gremlin-lang compatible String
     */
    public String getGremlin(final String g) {
        // special handling for CardinalityValueTraversal
        if (gremlin.length() != 0 && gremlin.charAt(0) != '.') {
            return gremlin.toString();
        }
        return g + gremlin;
    }

    /**
     * Get parameters used in Traversal.
     *
     * @return parameters Map
     */
    public Map<String, Object> getParameters() {
        return parameters;
    }

    /**
     * The alias to set.
     */
    public void addG(final String g) {
        parameters.put("g", g);
    }

    /**
    * Reset parameter naming counter. Mainly intended to make testing easier
    */
    public void reset() {
        paramCount.set(0);
    }

    /**
     * Add a {@link TraversalSource} instruction to the GremlinLang.
     *
     * @param sourceName the traversal source method name (e.g. withSack())
     * @param arguments  the traversal source method arguments
     */
    public void addSource(final String sourceName, final Object... arguments) {
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

            Configuration configuration;
            String strategyName;

            // special handling for TraversalStrategyProxy
            if (arguments[i] instanceof TraversalStrategyProxy) {
                configuration = ((TraversalStrategy) arguments[i]).getConfiguration();
                strategyName = ((TraversalStrategyProxy) arguments[i]).getStrategyName();
            } else {
                configuration = ((TraversalStrategy) arguments[i]).getConfiguration();
                strategyName = arguments[i].getClass().getSimpleName();
            }

            if (configuration.isEmpty()) {
                sb.append(strategyName);
            } else {
                sb.append("new ")
                        .append(strategyName)
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
        addToGremlin(stepName, arguments);
    }

    /**
     * Provides a way to get configuration of a Traversal.
     *
     * @return list of OptionsStrategy
     */
    public List<OptionsStrategy> getOptionsStrategies() {
        return optionsStrategies;
    }

    public boolean isEmpty() {
        return this.gremlin.length() == 0;
    }

    @Override
    public String toString() {
        return gremlin.toString();
    }

    // todo: clarify equality with parameters
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GremlinLang gremlinLang = (GremlinLang) o;
        return Objects.equals(gremlin.toString(), gremlinLang.gremlin.toString()) &&
                Objects.equals(parameters, gremlinLang.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gremlin, parameters);
    }

    @Override
    public GremlinLang clone() {
        try {
            final GremlinLang clone = (GremlinLang) super.clone();
            clone.parameters = new HashMap<>(parameters);
            clone.gremlin = new StringBuilder(gremlin.length());
            clone.gremlin.append(gremlin);
            clone.optionsStrategies = new ArrayList<>(this.optionsStrategies);
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private Object[] flattenArguments(final Object... arguments) {
        if (arguments == null || arguments.length == 0)
            return EMPTY_ARRAY;
        final List<Object> flatArguments = new ArrayList<>(arguments.length);
        for (final Object object : arguments) {
            if (object instanceof Object[]) {
                for (final Object nestObject : (Object[]) object) {
                    flatArguments.add(convertArgument(nestObject));
                }
            } else
                flatArguments.add(convertArgument(object));
        }
        return flatArguments.toArray();
    }

    private Object convertArgument(final Object argument) {
        if (argument instanceof Traversal) {
            // prevent use of "g" to spawn child traversals
            if (((Traversal) argument).asAdmin().getTraversalSource().isPresent())
                throw new IllegalStateException(String.format(
                        "The child traversal of %s was not spawned anonymously - use the __ class rather than a TraversalSource to construct the child traversal", argument));

            return ((Traversal) argument).asAdmin().getGremlinLang();
        } else if (argument instanceof Map) {
            final Map<Object, Object> map = new LinkedHashMap<>(((Map) argument).size());
            for (final Map.Entry<?, ?> entry : ((Map<?, ?>) argument).entrySet()) {
                map.put(convertArgument(entry.getKey()), convertArgument(entry.getValue()));
            }
            return map;
        } else if (argument instanceof List) {
            final List<Object> list = new ArrayList<>(((List) argument).size());
            for (final Object item : (List) argument) {
                list.add(convertArgument(item));
            }
            return list;
        } else if (argument instanceof Set) {
            final Set<Object> set = new LinkedHashSet<>(((Set) argument).size());
            for (final Object item : (Set) argument) {
                set.add(convertArgument(item));
            }
            return set;
        } else
            return argument;
    }
}
