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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A generalized custom serializer registry for vendors implementing a {@link Graph}.  Vendors should register
 * custom serializers to this registry via the {@link #register(Class, Class, Object)} method.  The serializers
 * registered depend on the {@link Io} implementations that are expected to be supported.  There are currently
 * just two core implementations in {@link GryoIo} and {@link GraphSONIo}.  Both of these should be supported
 * for full compliance with the test suite.  There is no need to use this class if the {@link Graph} does not
 * have custom classes that require serialization.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoRegistry {
    private final Map<Class<? extends Io>, List<Pair<Class, Object>>> registeredSerializers = new HashMap<>();

    /**
     * Add a "serializer" for the {@code Mapper}.  Note that what is accepted as a "serializer" is implementation
     * specific.  An {@link Io} implementation will consult this registry for "serializer" classes
     * it expects so refer to the {@link Io} implementation to understand what is expected for these values.
     *
     * @param clazz usually this is the class that is to be serialized - may be {@code null}
     * @param serializer a serializer implementation
     */
    public void register(final Class<? extends Io> ioClass, final Class clazz, final Object serializer) {
        if (!registeredSerializers.containsKey(ioClass))
            registeredSerializers.put(ioClass, new ArrayList<>());
        registeredSerializers.get(ioClass).add(Pair.with(clazz, serializer));
    }

    /**
     * Find a list of all the serializers registered to an {@link Io} class by the {@link Graph}.
     */
    public List<Pair<Class,Object>> find(final Class<? extends Io> builderClass) {
        if (!registeredSerializers.containsKey(builderClass)) return Collections.emptyList();
        return Collections.unmodifiableList(registeredSerializers.get(builderClass).stream()
                .collect(Collectors.toList()));
    }

    /**
     * Find a list of all the serializers, of a particular type, registered to an {@link Io} class by the
     * {@link Graph}.
     */
    public <S> List<Pair<Class,S>> find(final Class<? extends Io> builderClass, final Class<S> serializerType) {
        if (!registeredSerializers.containsKey(builderClass)) return Collections.emptyList();
        return Collections.unmodifiableList(registeredSerializers.get(builderClass).stream()
                .filter(p -> serializerType.isAssignableFrom(p.getValue1().getClass()))
                .map(p -> Pair.with(p.getValue0(), (S) p.getValue1()))
                .collect(Collectors.toList()));
    }
}
