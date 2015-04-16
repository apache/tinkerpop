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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoRegistry {
    private final Map<Class<? extends Io.Builder>, List<Pair<Class, Object>>> registeredSerializers = new HashMap<>();

    /**
     * Add a "serializer" for the {@code Mapper}.  Note that what is accepted as a "serializer" is implementation
     * specific.  Note that the {@link Io.Builder} implementation will consult this registry for "serializer" classes
     * it expects so consult the {@link Io.Builder} implementation to understand what is expected for these values.
     *
     * @param clazz usually this is the class that is to be serialized - may be {@code null}
     * @param serializer a serializer implementation
     */
    public void register(final Class<? extends Io.Builder> builderClass, final Class clazz, final Object serializer) {
        if (registeredSerializers.containsKey(builderClass))
            registeredSerializers.get(builderClass).add(Pair.with(clazz, serializer));
        else
            registeredSerializers.put(builderClass, Arrays.asList(Pair.with(clazz, serializer)));
    }

    public List<Pair<Class,Object>> find(final Class<? extends Io> builderClass) {
        return Collections.unmodifiableList(registeredSerializers.get(builderClass).stream()
                .collect(Collectors.toList()));
    }

    public <S> List<Pair<Class,S>> find(final Class<? extends Io> builderClass, final Class<S> c) {
        return Collections.unmodifiableList(registeredSerializers.get(builderClass).stream()
                .filter(p -> p.getValue1().getClass().isAssignableFrom(c))
                .map(p -> Pair.with(p.getValue0(), (S) p.getValue1()))
                .collect(Collectors.toList()));
    }
}
