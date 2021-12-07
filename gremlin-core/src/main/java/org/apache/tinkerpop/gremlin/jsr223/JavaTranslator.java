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

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class JavaTranslator<S extends TraversalSource, T extends Traversal.Admin<?, ?>> implements Translator.StepTranslator<S, T> {

    private final S traversalSource;
    private final Class<?> anonymousTraversal;
    private static final Map<Class<?>, Map<String, List<ReflectedMethod>>> GLOBAL_METHOD_CACHE = new ConcurrentHashMap<>();
    private final Map<Class<?>, Map<String,Method>> localMethodCache = new ConcurrentHashMap<>();
    private final Method anonymousTraversalStart;

    private JavaTranslator(final S traversalSource) {
        this.traversalSource = traversalSource;
        this.anonymousTraversal = traversalSource.getAnonymousTraversalClass().orElse(null);
        this.anonymousTraversalStart = getStartMethodFromAnonymousTraversal();
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
        if (BytecodeHelper.isGraphOperation(bytecode))
            throw new IllegalArgumentException("JavaTranslator cannot translate traversal operations");

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

    private Object translateObject(final Object object) {
        if (object instanceof Bytecode.Binding)
            return translateObject(((Bytecode.Binding) object).value());
        else if (object instanceof Bytecode) {
            try {
                final Traversal.Admin<?, ?> traversal = (Traversal.Admin) this.anonymousTraversalStart.invoke(null);
                for (final Bytecode.Instruction instruction : ((Bytecode) object).getStepInstructions()) {
                    invokeMethod(traversal, Traversal.class, instruction.getOperator(), instruction.getArguments());
                }
                return traversal;
            } catch (final Throwable e) {
                throw new IllegalStateException(e.getMessage());
            }
        } else if (object instanceof TraversalStrategyProxy) {
            final Map<String, Object> map = new HashMap<>();
            final Configuration configuration = ((TraversalStrategyProxy) object).getConfiguration();
            configuration.getKeys().forEachRemaining(key -> map.put(key, translateObject(configuration.getProperty(key))));
            return invokeStrategyCreationMethod(object, map);
        } else if (object instanceof Map) {
            final Map<Object, Object> map = object instanceof Tree ?
                    new Tree() :
                    object instanceof LinkedHashMap ?
                            new LinkedHashMap<>(((Map) object).size()) :
                            new HashMap<>(((Map) object).size());
            for (final Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                map.put(translateObject(entry.getKey()), translateObject(entry.getValue()));
            }
            return map;
        } else if (object instanceof List) {
            final List<Object> list = new ArrayList<>(((List) object).size());
            for (final Object o : (List) object) {
                list.add(translateObject(o));
            }
            return list;
        } else if (object instanceof BulkSet) {
            final BulkSet<Object> bulkSet = new BulkSet<>();
            for (final Map.Entry<?, Long> entry : ((BulkSet<?>) object).asBulk().entrySet()) {
                bulkSet.add(translateObject(entry.getKey()), entry.getValue());
            }
            return bulkSet;
        } else if (object instanceof Set) {
            final Set<Object> set = object instanceof LinkedHashSet ?
                    new LinkedHashSet<>(((Set) object).size()) :
                    new HashSet<>(((Set) object).size());
            for (final Object o : (Set) object) {
                set.add(translateObject(o));
            }
            return set;
        } else
            return object;
    }

    private Object invokeStrategyCreationMethod(final Object delegate, final Map<String, Object> map) {
        final Class<?> strategyClass = ((TraversalStrategyProxy) delegate).getStrategyClass();
        final Map<String, Method> methodCache = localMethodCache.computeIfAbsent(strategyClass, k -> {
            final Map<String, Method> cacheEntry = new HashMap<>();
            try {
                cacheEntry.put("instance", strategyClass.getMethod("instance"));
            } catch (NoSuchMethodException ignored) {
                // nothing - the strategy may not be constructed this way
            }

            try {
                cacheEntry.put("create", strategyClass.getMethod("create", Configuration.class));
            } catch (NoSuchMethodException ignored) {
                // nothing - the strategy may not be constructed this way
            }

            if (cacheEntry.isEmpty())
                throw new IllegalStateException(String.format("%s does can only be constructed with instance() or create(Configuration)", strategyClass.getSimpleName()));

            return cacheEntry;
        });

        try {
            return map.isEmpty() ?
                    methodCache.get("instance").invoke(null) :
                    methodCache.get("create").invoke(null, new MapConfiguration(map));
        } catch (final InvocationTargetException | IllegalAccessException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private Object invokeMethod(final Object delegate, final Class<?> returnType, final String methodName, final Object... arguments) {
        // populate method cache for fast access to methods in subsequent calls
        final Map<String, List<ReflectedMethod>> methodCache = GLOBAL_METHOD_CACHE.getOrDefault(delegate.getClass(), new HashMap<>());
        if (methodCache.isEmpty()) buildMethodCache(delegate, methodCache);

        // create a copy of the argument array so as not to mutate the original bytecode - no need to create a new
        // object if there are no arguments.
        final Object[] argumentsCopy = arguments.length > 0 ? new Object[arguments.length] : arguments;
        for (int i = 0; i < arguments.length; i++) {
            argumentsCopy[i] = translateObject(arguments[i]);
        }

        // without this initial check iterating an invalid methodName will lead to a null pointer and a less than
        // great error message for the user. 
        if (!methodCache.containsKey(methodName)) {
            throw new IllegalStateException(generateMethodNotFoundMessage(
                    "Could not locate method", delegate, methodName, argumentsCopy));
        }

        try {
            for (final ReflectedMethod methodx : methodCache.get(methodName)) {
                final Method method = methodx.method;
                if (returnType.isAssignableFrom(method.getReturnType())) {
                    final Parameter[] parameters = methodx.parameters;
                    if (parameters.length == argumentsCopy.length || methodx.hasVarArgs) {
                        final Object[] newArguments = new Object[parameters.length];
                        boolean found = true;
                        for (int i = 0; i < parameters.length; i++) {
                            if (parameters[i].isVarArgs()) {
                                final Class<?> parameterClass = parameters[i].getType().getComponentType();
                                if (argumentsCopy.length > i && argumentsCopy[i] != null && !parameterClass.isAssignableFrom(argumentsCopy[i].getClass())) {
                                    found = false;
                                    break;
                                }
                                final Object[] varArgs = (Object[]) Array.newInstance(parameterClass, argumentsCopy.length - i);
                                int counter = 0;
                                for (int j = i; j < argumentsCopy.length; j++) {
                                    varArgs[counter++] = argumentsCopy[j];
                                }
                                newArguments[i] = varArgs;
                                break;
                            } else {
                                // try to detect the right method by comparing the type of the parameter to the type
                                // of the argument. doesn't always work so well because of null arguments which don't
                                // bring their type in bytecode and rely on position. this doesn't seem to happen often
                                // ...luckily...because method signatures tend to be sufficiently unique and do not
                                // encourage whacky use - like g.V().has(null, null) is clearly invalid so we don't
                                // even need to try to sort that out. on the other hand g.V().has('name',null) which
                                // is valid hits like four different possible overloads, but we can rely on the most
                                // generic one which takes Object as the second parameter. that seems to work in this
                                // case, but it's a shame this isn't nicer. seems like nicer would mean a heavy
                                // overhaul to Gremlin or to GLVs/bytecode and/or to serialization mechanisms.
                                //
                                // the check where argumentsCopy[i] is null could be accompanied by a type check for
                                // allowable signatures like:
                                // null == argumentsCopy[i] && parameters[i].getType() == Object.class
                                // but that doesn't seem helpful. perhaps this approach is fine as long as we ensure
                                // consistency of null calls to all overloads. in other words addV(String) must behave
                                // the same as addV(Traversal) if null is used as the argument. so far, that seems to
                                // be the case. if we find that is not happening we either fix that specific
                                // inconsistency, start special casing those method finds here, or as mentioned above
                                // do something far more drastic that doesn't involve reflection.
                                if (i < argumentsCopy.length && (null == argumentsCopy[i] ||
                                        (argumentsCopy[i] != null && (
                                        parameters[i].getType().isAssignableFrom(argumentsCopy[i].getClass()) ||
                                                (parameters[i].getType().isPrimitive() &&
                                                        (Number.class.isAssignableFrom(argumentsCopy[i].getClass()) ||
                                                                argumentsCopy[i].getClass().equals(Boolean.class) ||
                                                                argumentsCopy[i].getClass().equals(Byte.class) ||
                                                                argumentsCopy[i].getClass().equals(Character.class))))))) {
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
            throw new IllegalStateException(generateMethodNotFoundMessage(
                    e.getMessage(), null, methodName, argumentsCopy), e);
        }

        // if it got down here then the method was in the cache but it was never called as it could not be found
        // for the supplied arguments
        throw new IllegalStateException(generateMethodNotFoundMessage(
                "Could not locate method", delegate, methodName, argumentsCopy));
    }

    /**
     * Generates the message used when a method cannot be located in the translation. Arguments are converted to
     * classes to avoid exposing data that might be sensitive.
     */
    private String generateMethodNotFoundMessage(final String message, final Object delegate,
                                                 final String methodNameNotFound, final Object[] args) {
        final Object[] arguments = null == args ? new Object[0] : args;
        final String delegateClassName = delegate != null ? delegate.getClass().getSimpleName() : "";
        return message + ": " + delegateClassName + "." + methodNameNotFound + "(" +
                Stream.of(arguments).map(a -> null == a ? "null" : a.getClass().getSimpleName()).collect(Collectors.joining(", ")) + ")";
    }

    private synchronized static void buildMethodCache(final Object delegate, final Map<String, List<ReflectedMethod>> methodCache) {
        if (methodCache.isEmpty()) {
            for (final Method method : delegate.getClass().getMethods()) {
                final List<ReflectedMethod> list = methodCache.computeIfAbsent(method.getName(), k -> new ArrayList<>());
                list.add(new ReflectedMethod(method));
            }
            GLOBAL_METHOD_CACHE.put(delegate.getClass(), methodCache);
        }
    }

    private Method getStartMethodFromAnonymousTraversal() {
        if (this.anonymousTraversal != null) {
            try {
                return this.anonymousTraversal.getMethod("start");
            } catch (NoSuchMethodException ignored) {
            }
        }

        return null;
    }

    private static final class ReflectedMethod {
        private final Method method;
        private final Parameter[] parameters;
        private final boolean hasVarArgs;

        public ReflectedMethod(final Method m) {
            this.method = m;

            // the reflection getParameters() method calls clone() every time to get the Parameter array. caching it
            // saves a lot of extra processing
            this.parameters = m.getParameters();

            this.hasVarArgs = parameters.length > 0 && parameters[parameters.length - 1].isVarArgs();
        }
    }
}
