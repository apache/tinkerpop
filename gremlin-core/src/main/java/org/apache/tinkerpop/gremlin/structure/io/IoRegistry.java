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
 * A generalized custom serializer registry for providers implementing a {@link Graph}.  Providers should develop an
 * implementation of this interface if their implementation requires custom serialization of identifiers or other
 * such content housed in their graph.  Consider extending from {@link AbstractIoRegistry} and ensure that the
 * implementation has a zero-arg constructor or a static "instance" method that returns an {@code IoRegistry}
 * instance, as implementations may need to be constructed from reflection given different parts of the TinkerPop
 * stack.
 * <p/>
 * The serializers to register depend on the {@link Io} implementations that are expected to be supported.  There
 * are currently two core implementations in {@link GryoIo} and {@link GraphSONIo}.  Both of these should be supported
 * for full compliance with the test suite.
 * <p/>
 * There is no need to use this class if the {@link Graph} does not have custom classes that require serialization.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface IoRegistry {

    public static final String IO_REGISTRY = "gremlin.io.registry";

    /**
     * Find a list of all the serializers registered to an {@link Io} class by the {@link Graph}.
     */
    public List<Pair<Class,Object>> find(final Class<? extends Io> builderClass);

    /**
     * Find a list of all the serializers, of a particular type, registered to an {@link Io} class by the
     * {@link Graph}.
     */
    public <S> List<Pair<Class,S>> find(final Class<? extends Io> builderClass, final Class<S> serializerType);
}
