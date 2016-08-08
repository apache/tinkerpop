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

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroovyTranslator implements Translator.ScriptTranslator {

    private final String traversalSource;
    private final String anonymousTraversal;

    private GroovyTranslator(final String traversalSource, final String anonymousTraversal) {
        this.traversalSource = traversalSource;
        this.anonymousTraversal = anonymousTraversal;
    }

    public static final GroovyTranslator of(final String traversalSource, final String anonymousTraversal) {
        return new GroovyTranslator(traversalSource, anonymousTraversal);
    }

    public static final GroovyTranslator of(final String traversalSource) {
        return new GroovyTranslator(traversalSource, "__");
    }

    ///////

    @Override
    public String translate(final Bytecode bytecode) {
        return this.internalTranslate(this.traversalSource, bytecode);
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

    @Override
    public String getAnonymousTraversal() {
        return this.anonymousTraversal;
    }

    ///////

    private String internalTranslate(final String start, final Bytecode bytecode) {
        final StringBuilder traversalScript = new StringBuilder(start);
        final Bytecode clone = BytecodeHelper.filterInstructions(bytecode,
                instruction -> !Arrays.asList("withTranslator", "withStrategies").contains(instruction.getOperator()));
        for (final Bytecode.Instruction instruction : clone.getSourceInstructions()) {
            processInstruction(traversalScript, instruction);
        }
        for (final Bytecode.Instruction instruction : clone.getStepInstructions()) {
            processInstruction(traversalScript, instruction);
        }
        final String script = traversalScript.toString();
        if (script.contains("$"))
            throw new VerificationException("Lambdas are currently not supported: " + script, EmptyTraversal.instance());
        return script;
    }

    private void processInstruction(final StringBuilder traversalScript, final Bytecode.Instruction instruction) {
        final String methodName = instruction.getOperator();
        final Object[] arguments = instruction.getArguments();
        final List<Object> objects = Arrays.asList(arguments);
        if (objects.isEmpty())
            traversalScript.append(".").append(methodName).append("()");
        else {
            traversalScript.append(".");
            String temp = methodName + "(";
            for (final Object object : objects) {
                temp = temp + convertToString(object) + ",";
            }
            traversalScript.append(temp.substring(0, temp.length() - 1) + ")");
        }
    }

    private String convertToString(final Object object) {
        if (object instanceof Bytecode.Binding)
            return ((Bytecode.Binding) object).variable();
        else if (object instanceof String)
            return "\"" + object + "\"";
        else if (object instanceof List) {
            final List<String> list = new ArrayList<>(((List) object).size());
            for (final Object item : (List) object) {
                list.add(convertToString(item));
            }
            return list.toString();
        } else if (object instanceof Long)
            return object + "L";
        else if (object instanceof Double)
            return object + "d";
        else if (object instanceof Float)
            return object + "f";
        else if (object instanceof Integer)
            return "(int) " + object;
        else if (object instanceof Class)
            return ((Class) object).getCanonicalName();
        else if (object instanceof P)
            return convertPToString((P) object, new StringBuilder()).toString();
        else if (object instanceof SackFunctions.Barrier)
            return "SackFunctions.Barrier." + object.toString();
        else if (object instanceof VertexProperty.Cardinality)
            return "VertexProperty.Cardinality." + object.toString();
        else if (object instanceof Enum)
            return ((Enum) object).getDeclaringClass().getSimpleName() + "." + object.toString();
        else if (object instanceof Element)
            return convertToString(((Element) object).id()); // hack
        else if (object instanceof Computer) { // TODO: blow out
            return "";
        } else if (object instanceof Lambda) {
            final String lambdaString = ((Lambda) object).getLambdaScript();
            return lambdaString.startsWith("{") ? lambdaString : "{" + lambdaString + "}";
        } else if (object instanceof Bytecode)
            return this.internalTranslate(this.anonymousTraversal, (Bytecode) object);
        else
            return null == object ? "null" : object.toString();
    }

    private StringBuilder convertPToString(final P p, final StringBuilder current) {
        if (p instanceof ConnectiveP) {
            final List<P<?>> list = ((ConnectiveP) p).getPredicates();
            for (int i = 0; i < list.size(); i++) {
                convertPToString(list.get(i), current);
                if (i < list.size() - 1)
                    current.append(p instanceof OrP ? ".or(" : ".and(");
            }
            current.append(")");
        } else
            current.append("P.").append(p.getBiPredicate().toString()).append("(").append(convertToString(p.getValue())).append(")");
        return current;
    }
}
