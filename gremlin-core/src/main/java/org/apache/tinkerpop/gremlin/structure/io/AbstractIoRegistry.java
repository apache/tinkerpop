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
package org.apache.tinkerpop.gremlin.structure.io;

import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base implementation of the {@link IoRegistry} interface that provides implementation of the methods using a
 * a {@code HashMap}.  Providers should extend from this class if they have custom serializers to provide to IO
 * instances.  Simply register their classes to it via the provided {@link #register(Class, Class, Object)} method.
 * <p/>
 * It is important that implementations provide a zero-arg constructor as there may be cases where it will need to be
 * constructed via reflection.  As such, calls to {@link #register(Class, Class, Object)} should likely occur
 * directly in the constructor.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractIoRegistry implements IoRegistry {
    private final Map<Class<? extends Io>, List<Pair<Class, Object>>> registeredSerializers = new HashMap<>();

    /**
     * Add a "serializer" for the {@code Mapper}.  Note that what is accepted as a "serializer" is implementation
     * specific.  An {@link Io} implementation will consult this registry for "serializer" classes
     * it expects so refer to the {@link Io} implementation to understand what is expected for these values.
     *
     * @param clazz usually this is the class that is to be serialized - may be {@code null}
     * @param serializer a serializer implementation
     */
    protected void register(final Class<? extends Io> ioClass, final Class clazz, final Object serializer) {
        if (!registeredSerializers.containsKey(ioClass))
            registeredSerializers.put(ioClass, new ArrayList<>());
        registeredSerializers.get(ioClass).add(Pair.with(clazz, serializer));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Pair<Class,Object>> find(final Class<? extends Io> builderClass) {
        if (!registeredSerializers.containsKey(builderClass)) return Collections.emptyList();
        return Collections.unmodifiableList(registeredSerializers.get(builderClass).stream()
                .collect(Collectors.toList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <S> List<Pair<Class,S>> find(final Class<? extends Io> builderClass, final Class<S> serializerType) {
        if (!registeredSerializers.containsKey(builderClass)) return Collections.emptyList();
        return Collections.unmodifiableList(registeredSerializers.get(builderClass).stream()
                .filter(p -> serializerType.isAssignableFrom(p.getValue1().getClass()))
                .map(p -> Pair.with(p.getValue0(), (S) p.getValue1()))
                .collect(Collectors.toList()));
    }
}
