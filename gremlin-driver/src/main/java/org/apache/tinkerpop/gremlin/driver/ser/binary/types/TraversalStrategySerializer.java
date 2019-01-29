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
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalStrategySerializer extends SimpleTypeSerializer<TraversalStrategy> {

    public TraversalStrategySerializer() {
        super(DataType.TRAVERSALSTRATEGY);
    }

    @Override
    protected TraversalStrategy readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        final Class<TraversalStrategy> clazz = context.readValue(buffer, Class.class, false);
        final Map config = context.readValue(buffer, Map.class, false);

        return new TraversalStrategyProxy(clazz, new MapConfiguration(config));
    }

    @Override
    protected ByteBuf writeValue(final TraversalStrategy value, final ByteBufAllocator allocator, final GraphBinaryWriter context) throws SerializationException {
        final CompositeByteBuf result = allocator.compositeBuffer(2);
        result.addComponent(true, context.writeValue(value.getClass(), allocator, false));
        result.addComponent(true, context.writeValue(translateToBytecode(ConfigurationConverter.getMap(value.getConfiguration())), allocator, false));
        return result;
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
