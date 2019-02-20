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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded.ShadedSerializerAdapter;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;

import java.util.function.Function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class GryoTypeReg<T> implements TypeRegistration<T> {

    private final Class<T> clazz;
    private final Serializer<T> shadedSerializer;
    private final SerializerShim<T> serializerShim;
    private final Function<Kryo, Serializer> functionOfShadedKryo;
    private final int id;

    private GryoTypeReg(final Class<T> clazz,
                        final Serializer<T> shadedSerializer,
                        final SerializerShim<T> serializerShim,
                        final Function<Kryo, Serializer> functionOfShadedKryo,
                        final int id) {
        if (null == clazz) throw new IllegalArgumentException("clazz cannot be null");

        this.clazz = clazz;
        this.shadedSerializer = shadedSerializer;
        this.serializerShim = serializerShim;
        this.functionOfShadedKryo = functionOfShadedKryo;
        this.id = id;

        int serializerCount = 0;
        if (null != this.shadedSerializer)
            serializerCount++;
        if (null != this.serializerShim)
            serializerCount++;
        if (null != this.functionOfShadedKryo)
            serializerCount++;

        if (1 < serializerCount) {
            final String msg = String.format(
                    "GryoTypeReg accepts at most one kind of serializer, but multiple " +
                            "serializers were supplied for class %s (id %s).  " +
                            "Shaded serializer: %s.  Shim serializer: %s.  Shaded serializer function: %s.",
                    this.clazz.getCanonicalName(), id,
                    this.shadedSerializer, this.serializerShim, this.functionOfShadedKryo);
            throw new IllegalArgumentException(msg);
        }
    }

    static <T> GryoTypeReg<T> of(final Class<T> clazz, final int id) {
        return new GryoTypeReg<>(clazz, null, null, null, id);
    }

    static <T> GryoTypeReg<T> of(final Class<T> clazz, final int id, final Serializer<T> shadedSerializer) {
        return new GryoTypeReg<>(clazz, shadedSerializer, null, null, id);
    }

    static <T> GryoTypeReg<T> of(final Class<T> clazz, final int id, final SerializerShim<T> serializerShim) {
        return new GryoTypeReg<>(clazz, null, serializerShim, null, id);
    }

    static <T> GryoTypeReg<T> of(final Class clazz, final int id, final Function<Kryo, Serializer> fct) {
        return new GryoTypeReg<>(clazz, null, null, fct, id);
    }

    @Override
    public Serializer<T> getShadedSerializer() {
        return shadedSerializer;
    }

    @Override
    public SerializerShim<T> getSerializerShim() {
        return serializerShim;
    }

    @Override
    public Function<Kryo, Serializer> getFunctionOfShadedKryo() {
        return functionOfShadedKryo;
    }

    @Override
    public Class<T> getTargetClass() {
        return clazz;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public Kryo registerWith(final Kryo kryo) {
        if (null != functionOfShadedKryo)
            kryo.register(clazz, functionOfShadedKryo.apply(kryo), id);
        else if (null != shadedSerializer)
            kryo.register(clazz, shadedSerializer, id);
        else if (null != serializerShim)
            kryo.register(clazz, new ShadedSerializerAdapter<>(serializerShim), id);
        else {
            kryo.register(clazz, kryo.getDefaultSerializer(clazz), id);
            // Suprisingly, the preceding call is not equivalent to
            //   kryo.register(clazz, id);
        }

        return kryo;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("targetClass", clazz)
                .append("id", id)
                .append("shadedSerializer", shadedSerializer)
                .append("serializerShim", serializerShim)
                .append("functionOfShadedKryo", functionOfShadedKryo)
                .toString();
    }
}
