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
package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PSerializer<T extends P> extends SimpleTypeSerializer<T> {

    private final Class<T> classOfP;

    public PSerializer(final DataType typeOfP, final Class<T> classOfP) {
        super(typeOfP);
        this.classOfP = classOfP;
    }

    @Override
    T readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        final String predicateName = context.readValue(buffer, String.class, false);
        final int length = context.readValue(buffer, Integer.class, false);
        final Object[] args = new Object[length];
        final Class<?>[] argumentClasses = new Class[length];
        boolean collectionType = false;
        for (int i = 0; i < length; i++) {
            args[i] = context.read(buffer);
            argumentClasses[i] = args[i].getClass();
        }

        switch (predicateName) {
            case "and":
                return (T) ((P) args[0]).and((P) args[1]);
            case "or":
                return (T) ((P) args[0]).or((P) args[1]);
            default:
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
                            throw new SerializationException("not found");
                        }
                    }
                }

                try {
                    if (Modifier.isStatic(m.getModifiers())) {
                        if (collectionType)
                            return (T) m.invoke(null, Arrays.asList(args));
                        else
                            return (T) m.invoke(null, args);
                    } else {
                        // try an instance method as it might be a form of ConnectiveP which means there is a P as an
                        // argument that should be used as the object of an instance method
                        if (args.length != 2 || !(args[0] instanceof P) || !(args[1] instanceof P))
                            throw new IllegalStateException(String.format("Could not determine the form of P for %s and %s",
                                    predicateName, Arrays.asList(args)));

                        final P firstP = (P) args[0];
                        final P secondP = (P) args[1];

                        return (T) m.invoke(firstP, secondP);
                    }
                } catch (Exception ex) {
                    throw new SerializationException(ex);
                }
        }
    }

    @Override
    public ByteBuf writeValue(final T value, final ByteBufAllocator allocator, final GraphBinaryWriter context) throws SerializationException {
        // the predicate name is either a static method of P or an instance method when a type ConnectiveP
        final boolean isConnectedP = value instanceof ConnectiveP;
        final String predicateName = isConnectedP ?
                (value instanceof AndP ? "and" : "or") :
                value.getBiPredicate().toString();
        final Object args = isConnectedP ? ((ConnectiveP<?>) value).getPredicates() : value.getValue();

        final List<Object> argsAsList = args instanceof Collection ? new ArrayList<>((Collection) args) : Collections.singletonList(args);
        final int length = argsAsList.size();
        final CompositeByteBuf result = allocator.compositeBuffer(2 + length);

        result.addComponent(true, context.writeValue(predicateName, allocator, false));
        result.addComponent(true, context.writeValue(length, allocator, false));

        for (Object o : argsAsList) {
            result.addComponent(true, context.write(o, allocator));
        }

        return result;
    }
}
