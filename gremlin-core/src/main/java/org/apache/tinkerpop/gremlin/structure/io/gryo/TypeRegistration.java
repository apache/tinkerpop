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

import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;

/**
 * Represents a class serializable with Gryo.
 * <p>
 * At most one of the {@link #getShadedSerializer()}, {@link #getSerializerShim()},
 * and {@link #getFunctionOfShadedKryo()} will return a non-null value.  If all
 * three methods return null, then there is no custom serialization logic associated
 * with this class.  Gryo/Kryo will use its default serializer.
 *
 * @param <T> the serializable type
 */
public interface TypeRegistration<T> {

    /**
     * @return the serializable class this instance describes
     */
    Class<T> getTargetClass();

    /**
     * @return numeric identifier used as a shorthand for this type in Gryo's serialized form
     */
    int getId();

    /**
     * @return the shaded-Kryo serializer that handles this type, if one is defined
     */
    Serializer<T> getShadedSerializer();

    /**
     * @return the shim-Kryo serializer that handles this type, if one is defined
     */
    SerializerShim<T> getSerializerShim();

    /**
     * @return a function that accepts a shaded-Kryo instance and returns a serializer, if such a function is defined
     */
    java.util.function.Function<Kryo, Serializer> getFunctionOfShadedKryo();

    /**
     * Registers this type on the supplied {@link Kryo} instance, using whatever custom serializer
     * may be present, then returns the same {@linkplain Kryo} instance supplied as the parameter.
     *
     * @param kryo Kryo instance into which this type is registered
     * @return the sole parameter
     */
    Kryo registerWith(final Kryo kryo);

    /**
     * Returns true if at least one of {@link #getShadedSerializer()}, {@link #getSerializerShim()}, or
     * {@link #getFunctionOfShadedKryo()} is non null.  Returns false if all are null.
     *
     * @return whether a serializer is defined for this type registration
     */
    default boolean hasSerializer() {
        return null != getFunctionOfShadedKryo() ||
                null != getSerializerShim() ||
                null != getShadedSerializer();
    }
}
