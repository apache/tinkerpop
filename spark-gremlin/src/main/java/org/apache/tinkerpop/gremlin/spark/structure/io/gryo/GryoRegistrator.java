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
package org.apache.tinkerpop.gremlin.spark.structure.io.gryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.util.collection.CompactBuffer;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopEdge;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopProperty;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopVertex;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopVertexProperty;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.MessagePayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewIncomingPayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewOutgoingPayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewPayload;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.kryoshim.unshaded.UnshadedSerializerAdapter;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoSerializersV1d0;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.TypeRegistration;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded.ShadedSerializerAdapter;
import org.apache.tinkerpop.gremlin.structure.io.util.IoRegistryHelper;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.util.SystemUtil;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.mutable.WrappedArray;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * A spark.kryo.registrator implementation that installs TinkerPop types.
 * This is intended for use with spark.serializer=KryoSerializer, not GryoSerializer.
 */
public class GryoRegistrator implements KryoRegistrator {

    private static final Logger log = LoggerFactory.getLogger(GryoRegistrator.class);

    @Override
    public void registerClasses(final Kryo kryo) {
        registerClasses(kryo, Collections.emptyMap(), Collections.emptySet());
    }

    /**
     * Register TinkerPop's classes with the supplied {@link Kryo} instance
     * while honoring optional overrides and optional class blacklist ("blackset"?).
     *
     * @param kryo                the Kryo serializer instance with which to register types
     * @param serializerOverrides serializer mappings that override this class's defaults
     * @param blacklist           classes which should not be registered at all, even if there is an override entry
     *                            or if they would be registered by this class by default (does not affect Kryo's
     *                            built-in registrations, e.g. String.class).
     */
    public void registerClasses(final Kryo kryo, final Map<Class<?>, Serializer<?>> serializerOverrides, final Set<Class<?>> blacklist) {
        // Apply TinkerPop type registrations copied from GyroSerializer's constructor
        for (Map.Entry<Class<?>, Serializer<?>> ent : getExtraRegistrations().entrySet()) {
            final Class<?> targetClass = ent.getKey();
            final Serializer<?> ser = ent.getValue();

            // Is this class blacklisted?  Skip it. (takes precedence over serializerOverrides)
            if (blacklist.contains(targetClass)) {
                log.debug("Not registering serializer for {} (blacklisted)", targetClass);
                continue;
            }

            if (checkForAndApplySerializerOverride(serializerOverrides, kryo, targetClass)) {
                // do nothing but skip the remaining else(-if) clauses
            } else if (null == ser) {
                log.debug("Registering {} with default serializer", targetClass);
                kryo.register(targetClass);
            } else {
                log.debug("Registering {} with serializer {}", targetClass, ser);
                kryo.register(targetClass, ser);
            }
        }

        final Set<Class<?>> shimmedClassesFromGryoMapper = new HashSet<>();

        // Apply GryoMapper's default registrations
        for (TypeRegistration<?> tr : GryoMapper.build().version(GryoVersion.V1_0).create().getTypeRegistrations()) {
            // Is this class blacklisted?  Skip it. (takes precedence over serializerOverrides)
            if (blacklist.contains(tr.getTargetClass())) {
                log.debug("Not registering serializer for {} (blacklisted)", tr.getTargetClass());
                continue;
            }

            final org.apache.tinkerpop.shaded.kryo.Serializer<?> shadedSerializer = tr.getShadedSerializer();
            final SerializerShim<?> serializerShim = tr.getSerializerShim();
            final java.util.function.Function<
                    org.apache.tinkerpop.shaded.kryo.Kryo,
                    org.apache.tinkerpop.shaded.kryo.Serializer> functionOfShadedKryo = tr.getFunctionOfShadedKryo();

            // Apply overrides with the highest case-precedence
            if (checkForAndApplySerializerOverride(serializerOverrides, kryo, tr.getTargetClass())) {
                // do nothing but skip the remaining else(-if) clauses
            } else if (null != shadedSerializer) {
                if (shadedSerializer.getClass().equals(org.apache.tinkerpop.shaded.kryo.serializers.JavaSerializer.class)) {
                    // Convert GryoMapper's shaded JavaSerializer mappings to their unshaded equivalents
                    log.debug("Registering {} with JavaSerializer", tr.getTargetClass());
                    kryo.register(tr.getTargetClass(), new JavaSerializer());
                } else {
                    // There's supposed to be a check in GryoMapper that prevents this from happening
                    log.error("GryoMapper's default serialization registration for {} is a {}. " +
                                    "This is probably a bug in TinkerPop (this is not a valid default registration). " +
                                    "I am configuring Spark to use Kryo's default serializer for this class, " +
                                    "but this may cause serialization failures at runtime.",
                            tr.getTargetClass(),
                            org.apache.tinkerpop.shaded.kryo.Serializer.class.getCanonicalName());
                    kryo.register(tr.getTargetClass());
                }
            } else if (null != serializerShim) {
                // Wrap shim serializers in an adapter for Spark's unshaded Kryo
                log.debug("Registering {} to serializer shim {} (serializer shim {})",
                        tr.getTargetClass(), serializerShim, serializerShim.getClass());
                kryo.register(tr.getTargetClass(), new UnshadedSerializerAdapter<>(serializerShim));
                shimmedClassesFromGryoMapper.add(tr.getTargetClass());
            } else if (null != functionOfShadedKryo) {
                // As with shaded serializers, there's supposed to be a check in GryoMapper that prevents this from happening
                log.error("GryoMapper's default serialization registration for {} is a Function<{},{}>.  " +
                                "This is probably a bug in TinkerPop (this is not a valid default registration). " +
                                "I am configuring Spark to use Kryo's default serializer instead of this function, " +
                                "but this may cause serialization failures at runtime.",
                        tr.getTargetClass(),
                        org.apache.tinkerpop.shaded.kryo.Kryo.class.getCanonicalName(),
                        org.apache.tinkerpop.shaded.kryo.Serializer.class.getCanonicalName());
                kryo.register(tr.getTargetClass());
            } else {
                // Register all other classes with the default behavior (FieldSerializer)
                log.debug("Registering {} with default serializer", tr.getTargetClass());
                kryo.register(tr.getTargetClass());
            }
        }

        // StarGraph's shim serializer is especially important on Spark for efficiency reasons,
        // so log a warning if we failed to register it somehow
        if (!shimmedClassesFromGryoMapper.contains(StarGraph.class)) {
            log.warn("No SerializerShim found for StarGraph");
        }

        // handle io-registry classes
        for (final IoRegistry registry : IoRegistryHelper.createRegistries(SystemUtil.getSystemPropertiesConfiguration("tinkerpop", true))) {
            for (final Pair<Class, Object> pair : registry.find(GryoIo.class)) {
                if (pair.getValue1() instanceof SerializerShim)
                    kryo.register(pair.getValue0(), new UnshadedSerializerAdapter((SerializerShim) pair.getValue1()));
                else if (pair.getValue1() instanceof ShadedSerializerAdapter)
                    kryo.register(pair.getValue0(), new UnshadedSerializerAdapter(((ShadedSerializerAdapter) pair.getValue1()).getSerializerShim()));
                else
                    kryo.register(pair.getValue0(), kryo.getDefaultSerializer(pair.getValue0()));
            }
        }
    }

    private LinkedHashMap<Class<?>, Serializer<?>> getExtraRegistrations() {

        /* The map returned by this method MUST have a fixed iteration order!
         *
         * The order itself is irrelevant, so long as it is completely stable at runtime.
         *
         * LinkedHashMap satisfies this requirement (its contract specifies
         * iteration in key-insertion-order).
         */

        final LinkedHashMap<Class<?>, Serializer<?>> m = new LinkedHashMap<>();
        // The following entries were copied from GryoSerializer's constructor
        // This could be turned into a static collection on GryoSerializer to avoid
        // duplication, but it would be a bit cumbersome to do so without disturbing
        // the ordering of the existing entries in that constructor, since not all
        // of the entries are for TinkerPop (and the ordering is significant).
        try {
            m.put(Class.forName("scala.reflect.ClassTag$GenericClassTag"), new JavaSerializer());
            m.put(Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"), new JavaSerializer());
            m.put(Class.forName("org.apache.spark.internal.io.FileCommitProtocol$EmptyTaskCommitMessage$"), new JavaSerializer());

        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        m.put(WrappedArray.ofRef.class, null);
        m.put(MessagePayload.class, null);
        m.put(ViewIncomingPayload.class, null);
        m.put(ViewOutgoingPayload.class, null);
        m.put(ViewPayload.class, null);
        m.put(VertexWritable.class, new UnshadedSerializerAdapter<>(new VertexWritableSerializer()));
        m.put(ObjectWritable.class, new UnshadedSerializerAdapter<>(new ObjectWritableSerializer<>()));
        //
        m.put(HadoopVertex.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.VertexSerializer()));
        m.put(HadoopVertexProperty.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.VertexPropertySerializer()));
        m.put(HadoopProperty.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.PropertySerializer()));
        m.put(HadoopEdge.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.EdgeSerializer()));
        //
        m.put(ComputerGraph.ComputerVertex.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.VertexSerializer()));
        m.put(ComputerGraph.ComputerVertexProperty.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.VertexPropertySerializer()));
        m.put(ComputerGraph.ComputerProperty.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.PropertySerializer()));
        m.put(ComputerGraph.ComputerEdge.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.EdgeSerializer()));
        //
        m.put(StarGraph.StarEdge.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.EdgeSerializer()));
        m.put(StarGraph.StarVertex.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.VertexSerializer()));
        m.put(StarGraph.StarProperty.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.PropertySerializer()));
        m.put(StarGraph.StarVertexProperty.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.VertexPropertySerializer()));
        //
        m.put(MutablePath.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.PathSerializer()));
        m.put(ImmutablePath.class, new UnshadedSerializerAdapter<>(new GryoSerializersV1d0.PathSerializer()));
        //
        m.put(CompactBuffer[].class, null);
        // TODO: VoidSerializer is a default serializer and thus, may not be needed (if it is, you can't use FieldSerializer)
        // TODO: We will have to copy/paste the shaded DefaultSerializer.VoidSerializer into an unshaded form.
        //m.put(void.class, null);
        //m.put(Void.class, null);
        return m;
    }

    private boolean checkForAndApplySerializerOverride(final Map<Class<?>, Serializer<?>> serializerOverrides,
                                                       final Kryo kryo, Class<?> targetClass) {
        if (serializerOverrides.containsKey(targetClass)) {
            final Serializer<?> ser = serializerOverrides.get(targetClass);
            if (null == ser) {
                // null means use Kryo's default serializer
                log.debug("Registering {} with default serializer per overrides", targetClass);
                kryo.register(targetClass);
            } else {
                // nonnull means use that serializer
                log.debug("Registering {} with serializer {} per overrides", targetClass, ser);
                kryo.register(targetClass, ser);
            }
            return true;
        }
        return false;
    }
}
