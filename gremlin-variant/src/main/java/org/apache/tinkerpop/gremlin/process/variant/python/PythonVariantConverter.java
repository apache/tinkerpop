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
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.variant.VariantConverter;
import org.apache.tinkerpop.gremlin.process.variant.VariantGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.util.ScriptEngineCache;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PythonVariantConverter implements VariantConverter {

    private static ScriptEngine JYTHON_ENGINE = ScriptEngineCache.get("jython");

    static {
        try {
            GremlinPythonGenerator.create("/tmp");
            JYTHON_ENGINE.eval("execfile(\"/tmp/gremlin-python-3.2.1-SNAPSHOT.py\")");
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static final Set<String> PREFIX_NAMES = new HashSet<>(Arrays.asList("as", "in", "and", "or", "is", "not", "from"));

    @Override
    public String generateGremlinGroovy(final StringBuilder currentTraversal) throws ScriptException {
        if (currentTraversal.toString().contains("$") || currentTraversal.toString().contains("@"))
            throw new VerificationException("Lambda: " + currentTraversal.toString(), EmptyTraversal.instance());

        final Bindings jythonBindings = new SimpleBindings();
        jythonBindings.put("g", JYTHON_ENGINE.eval("PythonGraphTraversalSource(\"g\")"));
        JYTHON_ENGINE.getContext().setBindings(jythonBindings, ScriptContext.GLOBAL_SCOPE);
        System.out.println(currentTraversal.toString());
        return JYTHON_ENGINE.eval(currentTraversal.toString()).toString();
    }

    @Override
    public void addStep(final StringBuilder currentTraversal, final String stepName, final Object... arguments) {
        if (arguments.length == 0)
            currentTraversal.append(".").append(convertStepName(stepName)).append("()");
        else if (stepName.equals("range") && 2 == arguments.length)
            currentTraversal.append("[").append(arguments[0]).append(":").append(arguments[1]).append("]");
        else if (stepName.equals("limit") && 1 == arguments.length)
            currentTraversal.append("[0:").append(arguments[0]).append("]");
        else {
            String temp = "." + convertStepName(stepName) + "(";
            for (final Object object : arguments) {
                if (object instanceof Object[]) {
                    for (final Object object2 : (Object[]) object) {
                        temp = temp + convertToString(object2) + ",";
                    }
                } else {
                    temp = temp + convertToString(object) + ",";
                }
            }
            currentTraversal.append(temp.substring(0, temp.length() - 1) + ")");
        }
    }

    private static String convertToString(final Object object) {
        if (object instanceof String)
            return "\"" + object + "\"";
        else if (object instanceof List) {
            final List list = new ArrayList<>(((List) object).size());
            for (final Object item : (List) object) {
                list.add(item instanceof String ? "'" + item + "'" : convertToString(item));
            }
            return list.toString();
        } else if (object instanceof Long)
            return object + "L";
        else if (object instanceof SackFunctions.Barrier)
            return "\"SackFunctions.Barrier." + object.toString() + "\"";
        else if (object instanceof Direction)
            return "\"Direction." + object.toString() + "\"";
        else if (object instanceof Operator)
            return "\"Operator." + object.toString() + "\"";
        else if (object instanceof Pop)
            return "\"Pop." + object.toString() + "\"";
        else if (object instanceof Column)
            return "\"Column." + object.toString() + "\"";
        else if (object instanceof P)
            return "\"P." + ((P) object).getBiPredicate() + "(" + (((P) object).getValue() instanceof String ? "'" + ((P) object).getValue() + "'" : convertToString(((P) object).getValue())) + ")" + "\"";
        else if (object instanceof T)
            return "\"T." + object.toString() + "\"";
        else if (object instanceof Order)
            return "\"Order." + object.toString() + "\"";
        else if (object instanceof Scope)
            return "\"Scope." + object.toString() + "\"";
        else if (object instanceof Element)
            return convertToString(((Element) object).id()); // hack
        else if (object instanceof VariantGraphTraversal)
            return ((VariantGraphTraversal) object).getVariantString().toString();
        else if (object instanceof Boolean)
            return object.equals(Boolean.TRUE) ? "True" : "False";
        else
            return null == object ? "" : object.toString();
    }

    private static String convertStepName(final String stepName) {
        if (PREFIX_NAMES.contains(stepName))
            return "_" + stepName;
        else
            return stepName;
    }

}
