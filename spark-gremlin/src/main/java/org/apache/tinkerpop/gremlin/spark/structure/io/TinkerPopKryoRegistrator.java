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
package org.apache.tinkerpop.gremlin.spark.structure.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.base.Preconditions;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.MessagePayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewIncomingPayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewOutgoingPayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewPayload;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.ObjectWritableSerializer;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.VertexWritableSerializer;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.kryoshim.unshaded.UnshadedSerializerAdapter;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.TypeRegistration;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A spark.kryo.registrator implementation that installs TinkerPop types.
 * This is intended for use with spark.serializer=KryoSerializer, not GryoSerializer.
 */
public class TinkerPopKryoRegistrator implements KryoRegistrator {

    private static final Logger log = LoggerFactory.getLogger(TinkerPopKryoRegistrator.class);

    @Override
    public void registerClasses(Kryo kryo) {
        // TinkerPop type registrations copied from GyroSerializer's constructor
        kryo.register(MessagePayload.class);
        kryo.register(ViewIncomingPayload.class);
        kryo.register(ViewOutgoingPayload.class);
        kryo.register(ViewPayload.class);
        kryo.register(VertexWritable.class, new UnshadedSerializerAdapter<>(new VertexWritableSerializer()));
        kryo.register(ObjectWritable.class, new UnshadedSerializerAdapter<>(new ObjectWritableSerializer<>()));

        Set<Class<?>> shimmedClasses = new HashSet<>();

        Set<Class<?>> javaSerializationClasses = new HashSet<>();

        // Copy GryoMapper's default registrations
        for (TypeRegistration<?> tr : GryoMapper.build().create().getTypeRegistrations()) {
            // Special case for JavaSerializer, which is generally implemented in terms of TinkerPop's
            // problematic static GryoMapper/GryoSerializer pool (these are handled below the loop)
            org.apache.tinkerpop.shaded.kryo.Serializer<?> shadedSerializer = tr.getShadedSerializer();
            SerializerShim<?> serializerShim = tr.getSerializerShim();
            if (null != shadedSerializer &&
                    shadedSerializer.getClass().equals(org.apache.tinkerpop.shaded.kryo.serializers.JavaSerializer.class)) {
                javaSerializationClasses.add(tr.getTargetClass());
            } else if (null != serializerShim) {
                log.debug("Registering class {} to serializer shim {} (serializer shim class {})",
                        tr.getTargetClass(), serializerShim, serializerShim.getClass());
                kryo.register(tr.getTargetClass(), new UnshadedSerializerAdapter<>(serializerShim));
                shimmedClasses.add(tr.getTargetClass());
            } else {
                // Register with the default behavior (FieldSerializer)
                log.debug("Registering class {} with default serializer", tr.getTargetClass());
                kryo.register(tr.getTargetClass());
            }
        }

        Map<Class<?>, Serializer<?>> javaSerializerReplacements = new HashMap<>();
        javaSerializerReplacements.put(GroupStep.GroupBiOperator.class, new JavaSerializer());
        javaSerializerReplacements.put(OrderGlobalStep.OrderBiOperator.class, null);
        javaSerializerReplacements.put(TraversalExplanation.class, null);

        for (Map.Entry<Class<?>, Serializer<?>> e : javaSerializerReplacements.entrySet()) {
            Class<?> c = e.getKey();
            Serializer<?> s = e.getValue();

            if (javaSerializationClasses.remove(c)) {
                if (null != s) {
                    log.debug("Registering class {} with serializer {}", c, s);
                    kryo.register(c, s);
                } else {
                    log.debug("Registering class {} with default serializer", c);
                    kryo.register(c);
                }
            } else {
                log.debug("Registering class {} with JavaSerializer", c);
                kryo.register(c, new JavaSerializer());
            }
        }

        // We really care about StarGraph's shim serializer, so make sure we registered it
        if (!shimmedClasses.contains(StarGraph.class)) {
            log.warn("No SerializerShim found for StarGraph");
        }
    }
}
