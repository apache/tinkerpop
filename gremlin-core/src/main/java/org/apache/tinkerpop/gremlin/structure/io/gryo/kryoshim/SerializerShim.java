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
package org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim;

import org.apache.tinkerpop.gremlin.util.Serializer;

/**
 * A minimal {@link Serializer}-like abstraction. See that class for method documentation.
 *
 * @param <T> the class this serializer reads/writes from/to bytes.
 */
public interface SerializerShim<T> {

    public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final T object);

    public <I extends InputShim> T read(final KryoShim<I, ?> kryo, final I input, final Class<T> clazz);

    public default boolean isImmutable() {
        return false;
    }
}
