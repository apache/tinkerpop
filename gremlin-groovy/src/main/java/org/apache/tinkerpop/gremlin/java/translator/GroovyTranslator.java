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

package org.apache.tinkerpop.gremlin.java.translator;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.creation.TranslationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.util.TranslatorHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroovyTranslator implements Translator {

    private StringBuilder traversalScript;
    private final String alias;

    private GroovyTranslator(final String alias) {
        this.alias = alias;
        this.traversalScript = new StringBuilder(this.alias);
    }

    public static final GroovyTranslator of(final String alias) {
        return new GroovyTranslator(alias);
    }

    @Override
    public String getSourceLanguage() {
        return "gremlin-java";
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
    public String getAlias() {
        return this.alias;
    }

    @Override
    public void addStep(final Traversal.Admin<?, ?> traversal, final String stepName, final Object... arguments) {
        final List<Object> objects = TranslatorHelper.flattenArguments(arguments);
        if (objects.isEmpty())
            this.traversalScript.append(".").append(stepName).append("()");
        else {
            this.traversalScript.append(".");
            String temp = stepName + "(";
            for (final Object object : objects) {
                temp = temp + convertToString(object) + ",";
            }
            this.traversalScript.append(temp.substring(0, temp.length() - 1) + ")");
        }
    }

    @Override
    public Translator getAnonymousTraversalTranslator() {
        return new GroovyTranslator("__");
    }

    @Override
    public String getTraversalScript() {
        final String traversal = this.traversalScript.toString();
        if (traversal.contains("$"))
            throw new VerificationException("Lambdas are currently not supported: " + traversal, EmptyTraversal.instance());
        return traversal;
    }

    @Override
    public GroovyTranslator clone() {
        try {
            final GroovyTranslator clone = (GroovyTranslator) super.clone();
            clone.traversalScript = new StringBuilder(this.traversalScript);
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    ///////

    private static String convertToString(final Object object) {
        if (object instanceof String)
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
        } else if (object instanceof Traversal)
            return ((Traversal) object).asAdmin().getStrategies().getStrategy(TranslationStrategy.class).get().getTranslator().getTraversalScript();
        else
            return null == object ? "null" : object.toString();
    }

    private static StringBuilder convertPToString(final P p, final StringBuilder current) {
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
