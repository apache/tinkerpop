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

package org.apache.tinkerpop.gremlin.process.variant.python;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.variant.VariantConverter;
import org.apache.tinkerpop.gremlin.process.variant.VariantGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.apache.tinkerpop.gremlin.util.ScriptEngineCache;
import org.apache.tinkerpop.gremlin.util.iterator.ArrayIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PythonVariantConverter implements VariantConverter {

    private static ScriptEngine JYTHON_ENGINE = ScriptEngineCache.get("jython");
    private static boolean IMPORT_STATICS = new Random().nextBoolean();

    static {
        try {
            final String fileName = (new File("variants/python").exists() ? "variants/python/" : "gremlin-variants/variants/python/") + "gremlin-python-" + Gremlin.version() + ".py";
            GremlinPythonGenerator.create(fileName);
            JYTHON_ENGINE.eval("import sys");
            JYTHON_ENGINE.eval("sys.argv = [" + (IMPORT_STATICS ? "True" : "False") + "]");
            JYTHON_ENGINE.eval("execfile(\"" + fileName + "\")");
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static final Set<String> STEP_NAMES = Stream.of(GraphTraversal.class.getMethods()).filter(method -> Traversal.class.isAssignableFrom(method.getReturnType())).map(Method::getName).collect(Collectors.toSet());
    private static final Set<String> PREFIX_NAMES = new HashSet<>(Arrays.asList("as", "in", "and", "or", "is", "not", "from", "global"));
    private static final Set<String> NO_STATIC = Stream.of(T.values(), Operator.values())
            .flatMap(arg -> IteratorUtils.stream(new ArrayIterator<>(arg)))
            .map(arg -> ((Enum) arg).name())
            .collect(Collectors.toCollection(() -> new HashSet<>(Arrays.asList("not"))));


    public String generateGremlinGroovy(final StringBuilder currentTraversal) throws ScriptException {
        //if (IMPORT_STATICS) assert !currentTraversal.toString().contains("__");
        if (currentTraversal.toString().contains("$"))
            throw new VerificationException("Lambdas are currently not supported: " + currentTraversal.toString(), EmptyTraversal.instance());

        final Bindings jythonBindings = new SimpleBindings();
        jythonBindings.put("g", JYTHON_ENGINE.eval("PythonGraphTraversalSource(\"g\")"));
        JYTHON_ENGINE.getContext().setBindings(jythonBindings, ScriptContext.GLOBAL_SCOPE);
        return JYTHON_ENGINE.eval(currentTraversal.toString()).toString();
    }

    @Override
    public void addStep(final StringBuilder currentTraversal, final String stepName, final Object... arguments) {
        // flatten the arguments into a single array
        final Object[] objects = Stream.of(arguments)
                .flatMap(arg ->
                        IteratorUtils.stream(arg instanceof Object[] ?
                                new ArrayIterator<>((Object[]) arg) :
                                IteratorUtils.of(arg)))
                .toArray();
        if (objects.length == 0)
            currentTraversal.append(".").append(convertStepName(stepName)).append("()");
        else if (stepName.equals("range") && 2 == objects.length)
            currentTraversal.append("[").append(objects[0]).append(":").append(objects[1]).append("]");
        else if (stepName.equals("limit") && 1 == objects.length)
            currentTraversal.append("[0:").append(objects[0]).append("]");
        else if (stepName.equals("values") && 1 == objects.length && !currentTraversal.toString().equals("__") && !STEP_NAMES.contains(objects[0].toString()))
            currentTraversal.append(".").append(objects[0]);
        else {
            currentTraversal.append(".");
            String temp = convertStepName(stepName) + "(";
            for (final Object object : objects) {
                temp = temp + convertToString(object) + ",";
            }
            currentTraversal.append(temp.substring(0, temp.length() - 1) + ")");
        }
        if (IMPORT_STATICS && currentTraversal.toString().startsWith("__.")
                && !NO_STATIC.stream().filter(name -> currentTraversal.toString().contains(name)).findAny().isPresent())
            currentTraversal.delete(0, 3);
        if (!IMPORT_STATICS)
            assert currentTraversal.toString().startsWith("g.") || currentTraversal.toString().startsWith("__.");
    }

    private static String convertToString(final Object object) {
        if (object instanceof String)
            return "\"" + object + "\"";
        else if (object instanceof List) {
            final List list = new ArrayList<>(((List) object).size());
            for (final Object item : (List) object) {
                list.add(item instanceof String ? "'" + item + "'" : convertToString(item)); // hack
            }
            return list.toString();
        } else if (object instanceof Long)
            return object + "L";
        else if (object instanceof Class)
            return ((Class) object).getCanonicalName();
        else if (object instanceof SackFunctions.Barrier)
            return convertStatic("Barrier.") + object.toString();
        else if (object instanceof VertexProperty.Cardinality)
            return convertStatic("Cardinality.") + object.toString();
        else if (object instanceof Direction)
            return convertStatic("Direction.") + object.toString();
        else if (object instanceof Operator)
            return convertStatic("Operator.") + convertStepName(object.toString()); // to catch and/or
        else if (object instanceof Pop)
            return convertStatic("Pop.") + object.toString();
        else if (object instanceof Column)
            return convertStatic("Column.") + object.toString();
        else if (object instanceof P)
            return convertPToString((P) object, new StringBuilder()).toString();
        else if (object instanceof T)
            return convertStatic("T.") + object.toString();
        else if (object instanceof Order)
            return convertStatic("Order.") + object.toString();
        else if (object instanceof Scope)
            return convertStatic("Scope.") + convertStepName(object.toString()); // to catch global
        else if (object instanceof Element)
            return convertToString(((Element) object).id()); // hack
        else if (object instanceof VariantGraphTraversal)
            return ((VariantGraphTraversal) object).getVariantString().toString();
        else if (object instanceof Boolean)
            return object.equals(Boolean.TRUE) ? "True" : "False";
        else
            return null == object ? "" : object.toString();
    }

    private static String convertStatic(final String name) {
        return IMPORT_STATICS ? "" : name;
    }

    private static String convertStepName(final String stepName) {
        if (PREFIX_NAMES.contains(stepName))
            return "_" + stepName;
        else
            return stepName;
    }

    private static StringBuilder convertPToString(final P p, final StringBuilder current) {
        if (p instanceof ConnectiveP) {
            final List<P<?>> list = ((ConnectiveP) p).getPredicates();
            for (int i = 0; i < list.size(); i++) {
                convertPToString(list.get(i), current);
                if (i < list.size() - 1)
                    current.append(p instanceof OrP ? "._or(" : "._and(");
            }
            current.append(")");
        } else
            current.append(convertStatic("P.")).append(p.getBiPredicate().toString()).append("(").append(convertToString(p.getValue())).append(")");
        return current;
    }
}
