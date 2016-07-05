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

package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.ByteCode;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ReflectionTranslator implements Translator<Traversal.Admin<?, ?>> {

    private final TraversalSource source;

    public ReflectionTranslator(final TraversalSource source) {
        this.source = source;
    }

    @Override
    public String getAlias() {
        return null;
    }

    @Override
    public Traversal.Admin<?, ?> translate(final ByteCode byteCode) {
        TraversalSource tempSource = this.source;
        Traversal.Admin<?, ?> traversal = null == this.source ? __.start().asAdmin() : null;
        boolean fromSource = null == traversal;
        if (null != tempSource) {
            for (final ByteCode.Instruction instruction : byteCode.getSourceInstructions()) {
                tempSource = (TraversalSource) invokeMethod(tempSource, TraversalSource.class, instruction.getOperator(), instruction.getArguments());
            }
        }
        for (final ByteCode.Instruction instruction : byteCode.getStepInstructions()) {
            if (fromSource) {
                traversal = (Traversal.Admin) invokeMethod(tempSource, Traversal.class, instruction.getOperator(), instruction.getArguments());
                fromSource = false;
            } else
                invokeMethod(traversal, Traversal.class, instruction.getOperator(), instruction.getArguments());
        }
        return traversal;
    }

    @Override
    public String getSourceLanguage() {
        return null;
    }

    @Override
    public String getTargetLanguage() {
        return "gremlin-java";
    }

    ////

    private static Object invokeMethod(final Object delegate, final Class returnType, final String methodName, final Object... arguments) {
        try {
            for (final Method method : delegate.getClass().getMethods()) {
                if (method.getName().equals(methodName)) {
                    if (returnType.isAssignableFrom(method.getReturnType())) {
                        if (method.getParameterCount() == arguments.length || method.getParameters()[method.getParameters().length - 1].isVarArgs()) {
                            final Parameter[] parameters = method.getParameters();
                            final Object[] newArguments = new Object[parameters.length];
                            boolean found = true;
                            for (int i = 0; i < parameters.length; i++) {
                                if (parameters[i].isVarArgs()) {
                                    newArguments[i] = Arrays.copyOfRange(arguments, i, arguments.length, (Class) parameters[i].getType());
                                    break;
                                } else {
                                    if (arguments.length > 0 && (parameters[i].getType().isPrimitive() ||
                                            parameters[i].getType().isAssignableFrom(arguments[i].getClass()) ||
                                            parameters[i].getType().isAssignableFrom(Traversal.Admin.class) && arguments[i] instanceof ByteCode)) {
                                        newArguments[i] = arguments[i] instanceof ByteCode ?
                                                new ReflectionTranslator(null).translate((ByteCode) arguments[i]) :
                                                arguments[i];
                                    } else {
                                        found = false;
                                        break;
                                    }
                                }
                            }
                            if (found)
                                return 0 == newArguments.length ? method.invoke(delegate) : method.invoke(delegate, newArguments);
                        }
                    }
                }
            }
        } catch (Throwable e) {
            throw new IllegalStateException(e.getMessage() + methodName, e);
        }
        throw new IllegalStateException("could not find method: " + methodName + "--" + Arrays.toString(arguments));
    }
}
