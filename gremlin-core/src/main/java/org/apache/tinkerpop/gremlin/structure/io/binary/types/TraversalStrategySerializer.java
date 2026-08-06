/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io.binary.types;

import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyResolver;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalStrategySerializer extends SimpleTypeSerializer<TraversalStrategy> {

    private final TraversalStrategyResolver traversalStrategyResolver;

    public TraversalStrategySerializer() {
        this(TraversalStrategyResolver.defaultResolver());
    }

    public TraversalStrategySerializer(final TraversalStrategyResolver traversalStrategyResolver) {
        super(DataType.TRAVERSALSTRATEGY);
        this.traversalStrategyResolver = traversalStrategyResolver;
    }

    /**
     * Creates a serializer that preserves this serializer's allowed strategies and adds the supplied strategies.
     */
    public TraversalStrategySerializer withAllowedTraversalStrategies(
            final Collection<Class<? extends TraversalStrategy>> strategyClasses) {
        final TraversalStrategyResolver resolver = TraversalStrategyResolver.build().
                addAllowedTraversalStrategies(traversalStrategyResolver.getAllowedStrategies()).
                addAllowedTraversalStrategies(strategyClasses).create();
        return new TraversalStrategySerializer(resolver);
    }

    @Override
    protected TraversalStrategy readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final String strategyClassName = context.readValue(buffer, String.class, false);
        final Class<? extends TraversalStrategy> clazz = traversalStrategyResolver.resolve(strategyClassName);
        final Map config = context.readValue(buffer, Map.class, false);

        return new TraversalStrategyProxy(clazz, new MapConfiguration(config));
    }

    @Override
    protected void writeValue(final TraversalStrategy value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        final Class<? extends TraversalStrategy> strategyClass = value instanceof TraversalStrategyProxy ?
                ((TraversalStrategyProxy) value).getStrategyClass() :
                value.getClass();
        context.writeValue(strategyClass.getName(), buffer, false);
        context.writeValue(translateToBytecode(ConfigurationConverter.getMap(value.getConfiguration())), buffer, false);
    }

    private static Map<Object,Object> translateToBytecode(final Map<Object,Object> conf) {
        final Map<Object,Object> newConf = new LinkedHashMap<>(conf.size());
        conf.entrySet().forEach(entry -> {
            if (entry.getValue() instanceof Traversal)
                newConf.put(entry.getKey(), ((Traversal) entry.getValue()).asAdmin().getBytecode());
            else
                newConf.put(entry.getKey(), entry.getValue());
        });
        return newConf;
    }
}
