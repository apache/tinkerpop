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

package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class JavaTranslator<S extends TraversalSource, T extends Traversal.Admin<?, ?>> implements Translator.StepTranslator<S, T> {

    private final S traversalSource;
    private final Class anonymousTraversal;
    private static final Map<Class<? extends TraversalSource>, Map<String, List<Method>>> TRAVERSAL_SOURCE_METHOD_CACHE = new ConcurrentHashMap<>();
    private static final Map<Class<? extends Traversal>, Map<String, List<Method>>> TRAVERSAL_METHOD_CACHE = new ConcurrentHashMap<>();

    private JavaTranslator(final S traversalSource) {
        this.traversalSource = traversalSource;
        this.anonymousTraversal = traversalSource.getAnonymousTraversalClass().orElse(null);
    }

    public static <S extends TraversalSource, T extends Traversal.Admin<?, ?>> JavaTranslator<S, T> of(final S traversalSource) {
        return new JavaTranslator<>(traversalSource);
    }

    @Override
    public S getTraversalSource() {
        return this.traversalSource;
    }

    @Override
    public T translate(final Bytecode bytecode) {
        TraversalSource dynamicSource = this.traversalSource;
        Traversal.Admin<?, ?> traversal = null;
        for (final Bytecode.Instruction instruction : bytecode.getSourceInstructions()) {
            dynamicSource = (TraversalSource) invokeMethod(dynamicSource, TraversalSource.class, instruction.getOperator(), instruction.getArguments());
        }
        boolean spawned = false;
        for (final Bytecode.Instruction instruction : bytecode.getStepInstructions()) {
            if (!spawned) {
                traversal = (Traversal.Admin) invokeMethod(dynamicSource, Traversal.class, instruction.getOperator(), instruction.getArguments());
                spawned = true;
            } else
                invokeMethod(traversal, Traversal.class, instruction.getOperator(), instruction.getArguments());
        }
        return (T) traversal;
    }

    @Override
    public String getTargetLanguage() {
        return "gremlin-java";
    }

    @Override
    public String toString() {
        return StringFactory.translatorString(this);
    }

    ////

    private Traversal.Admin<?, ?> translateFromAnonymous(final Bytecode bytecode) {
        try {
            final Traversal.Admin<?, ?> traversal = (Traversal.Admin) this.anonymousTraversal.getMethod("start").invoke(null);
            for (final Bytecode.Instruction instruction : bytecode.getStepInstructions()) {
                invokeMethod(traversal, Traversal.class, instruction.getOperator(), instruction.getArguments());
            }
            return traversal;
        } catch (final Throwable e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

    private Object invokeMethod(final Object delegate, final Class returnType, final String methodName, final Object... arguments) {
        //////////////////////////
        //////////////////////////
        // populate method cache for fast access to methods in subsequent calls
        final Map<String, List<Method>> methodCache = delegate instanceof TraversalSource ?
                this.TRAVERSAL_SOURCE_METHOD_CACHE.getOrDefault(delegate.getClass(), new HashMap<>()) :
                this.TRAVERSAL_METHOD_CACHE.getOrDefault(delegate.getClass(), new HashMap<>());
        if (methodCache.isEmpty()) {
            for (final Method method : delegate.getClass().getMethods()) {
                if (!(method.getName().equals("addV") && method.getParameterCount() == 1 && method.getParameters()[0].getType().equals(Object[].class))) { // hack cause its hard to tell Object[] vs. String :|
                    List<Method> list = methodCache.get(method.getName());
                    if (null == list) {
                        list = new ArrayList<>();
                        methodCache.put(method.getName(), list);
                    }
                    list.add(method);
                }
            }
            if (delegate instanceof TraversalSource)
                TRAVERSAL_SOURCE_METHOD_CACHE.put((Class<TraversalSource>) delegate.getClass(), methodCache);
            else
                TRAVERSAL_METHOD_CACHE.put((Class<Traversal>) delegate.getClass(), methodCache);
        }
        //////////////////////////
        //////////////////////////

        // create a copy of the argument array so as not to mutate the original bytecode
        final Object[] argumentsCopy = new Object[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i] instanceof Bytecode.Binding)
                argumentsCopy[i] = ((Bytecode.Binding) arguments[i]).value();
            else if (arguments[i] instanceof Bytecode)
                argumentsCopy[i] = translateFromAnonymous((Bytecode) arguments[i]);
            else
                argumentsCopy[i] = arguments[i];
        }
        try {
            for (final Method method : methodCache.get(methodName)) {
                if (returnType.isAssignableFrom(method.getReturnType())) {
                    if (method.getParameterCount() == argumentsCopy.length || (method.getParameterCount() > 0 && method.getParameters()[method.getParameters().length - 1].isVarArgs())) {
                        final Parameter[] parameters = method.getParameters();
                        final Object[] newArguments = new Object[parameters.length];
                        boolean found = true;
                        for (int i = 0; i < parameters.length; i++) {
                            if (parameters[i].isVarArgs()) {
                                final Class<?> parameterClass = parameters[i].getType().getComponentType();
                                if (argumentsCopy.length > i && !parameterClass.isAssignableFrom(argumentsCopy[i].getClass())) {
                                    found = false;
                                    break;
                                }
                                Object[] varArgs = (Object[]) Array.newInstance(parameterClass, argumentsCopy.length - i);
                                int counter = 0;
                                for (int j = i; j < argumentsCopy.length; j++) {
                                    varArgs[counter++] = argumentsCopy[j];
                                }
                                newArguments[i] = varArgs;
                                break;
                            } else {
                                if (i < argumentsCopy.length &&
                                        (parameters[i].getType().isAssignableFrom(argumentsCopy[i].getClass()) ||
                                                (parameters[i].getType().isPrimitive() &&
                                                        (Number.class.isAssignableFrom(argumentsCopy[i].getClass()) ||
                                                                argumentsCopy[i].getClass().equals(Boolean.class) ||
                                                                argumentsCopy[i].getClass().equals(Byte.class) ||
                                                                argumentsCopy[i].getClass().equals(Character.class))))) {
                                    newArguments[i] = argumentsCopy[i];
                                } else {
                                    found = false;
                                    break;
                                }
                            }
                        }
                        if (found) {
                            return 0 == newArguments.length ? method.invoke(delegate) : method.invoke(delegate, newArguments);
                        }
                    }
                }
            }
        } catch (final Throwable e) {
            throw new IllegalStateException(e.getMessage() + ":" + methodName + "(" + Arrays.toString(argumentsCopy) + ")", e);
        }
        throw new IllegalStateException("Could not locate method: " + delegate.getClass().getSimpleName() + "." + methodName + "(" + Arrays.toString(argumentsCopy) + ")");
    }
}
