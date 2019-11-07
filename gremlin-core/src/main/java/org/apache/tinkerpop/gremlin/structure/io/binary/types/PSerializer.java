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
package org.apache.tinkerpop.gremlin.structure.io.binary.types;

import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PSerializer<T extends P> extends SimpleTypeSerializer<T> {

    private final Class<T> classOfP;

    private final ConcurrentHashMap<PFunctionId, CheckedFunction> methods = new ConcurrentHashMap<>();

    public PSerializer(final DataType typeOfP, final Class<T> classOfP) {
        super(typeOfP);
        this.classOfP = classOfP;
    }

    @Override
    protected T readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final String predicateName = context.readValue(buffer, String.class, false);
        final int length = context.readValue(buffer, Integer.class, false);
        final Object[] args = new Object[length];
        final Class<?>[] argumentClasses = new Class[length];
        for (int i = 0; i < length; i++) {
            args[i] = context.read(buffer);
            argumentClasses[i] = args[i].getClass();
        }
                    
        if ("and".equals(predicateName))
            return (T) ((P) args[0]).and((P) args[1]);
        else if ("or".equals(predicateName))
            return (T) ((P) args[0]).or((P) args[1]);
        else if ("not".equals(predicateName))
            return (T) P.not((P) args[0]);

        final CheckedFunction<Object[], T> f = getMethod(predicateName, argumentClasses);

        try {
            return f.apply(args);
        } catch (Exception ex) {
            throw new IOException(String.format("Can't deserialize value into the predicate: '%s'", predicateName), ex);
        }
    }

    private CheckedFunction getMethod(final String predicateName, final Class<?>[] argumentClasses) throws IOException {
        final PFunctionId id = new PFunctionId(predicateName, argumentClasses);

        CheckedFunction<Object[], T> result = methods.get(id);

        if (result == null) {
            boolean collectionType = false;
            Method m;
            try {
                // do a direct lookup
                m = classOfP.getMethod(predicateName, argumentClasses);
            } catch (NoSuchMethodException ex0) {
                // then try collection types
                try {
                    m = classOfP.getMethod(predicateName, Collection.class);
                    collectionType = true;
                } catch (NoSuchMethodException ex1) {
                    // finally go for the generics
                    try {
                        m = classOfP.getMethod(predicateName, Object.class);
                    } catch (NoSuchMethodException ex2) {
                        // finally go for the generics
                        try {
                            m = classOfP.getMethod(predicateName, Object.class, Object.class);
                        } catch (NoSuchMethodException ex3) {
                            throw new IOException(String.format("Can't find predicate method: '%s'", predicateName), ex2);
                        }
                    }
                }
            }

            final Method finalMethod = m;

            try {
                if (Modifier.isStatic(m.getModifiers())) {
                    if (collectionType) {
                        result = (args) -> (T) finalMethod.invoke(null, Arrays.asList(args));
                    } else {
                        result = (args) -> (T) finalMethod.invoke(null, args);
                    }
                } else {
                    // try an instance method as it might be a form of ConnectiveP which means there is a P as an
                    // argument that should be used as the object of an instance method
                    if (argumentClasses.length != 2) {
                        throw new IllegalStateException(String.format("Could not determine the form of P for %s and %s",
                                predicateName, Arrays.asList(argumentClasses)));
                    }

                    result = (args) -> {
                        if (!(args[0] instanceof P) || !(args[1] instanceof P))
                            throw new IllegalStateException(String.format("Could not determine the form of P for %s and %s",
                                    predicateName, Arrays.asList(args)));

                        final P firstP = (P) args[0];
                        final P secondP = (P) args[1];

                        return (T) finalMethod.invoke(firstP, secondP);
                    };
                }

                methods.put(id, result);
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }

        return result;
    }

    @Override
    protected void writeValue(final T value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        // the predicate name is either a static method of P or an instance method when a type ConnectiveP
        final boolean isConnectedP = value instanceof ConnectiveP;
        final String predicateName = isConnectedP ?
                (value instanceof AndP ? "and" : "or") :
                value.getBiPredicate().toString();
        final Object args = isConnectedP ? ((ConnectiveP<?>) value).getPredicates() : value.getValue();

        final List<Object> argsAsList = args instanceof Collection ? new ArrayList<>((Collection) args) : Collections.singletonList(args);
        final int length = argsAsList.size();

        context.writeValue(predicateName, buffer, false);
        context.writeValue(length, buffer, false);

        for (Object o : argsAsList) {
            context.write(o, buffer);
        }
    }

    @FunctionalInterface
    interface CheckedFunction<A, R> {
        R apply(A t) throws InvocationTargetException, IllegalAccessException;
    }

    class PFunctionId {
        private final String predicateName;
        private final Class<?>[] argumentClasses;

        PFunctionId(final String predicateName, final Class<?>[] argumentClasses) {
            this.predicateName = predicateName;
            this.argumentClasses = argumentClasses;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PFunctionId that = (PFunctionId) o;
            return predicateName.equals(that.predicateName) &&
                    Arrays.equals(argumentClasses, that.argumentClasses);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(predicateName);
            result = 31 * result + Arrays.hashCode(argumentClasses);
            return result;
        }
    }
}
