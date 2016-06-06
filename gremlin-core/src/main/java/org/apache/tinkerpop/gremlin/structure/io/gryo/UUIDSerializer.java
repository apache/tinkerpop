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

import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.OutputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;

import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class UUIDSerializer implements SerializerShim<UUID> {

    public UUIDSerializer() { }

    @Override
    public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final UUID uuid) {
        output.writeLong(uuid.getMostSignificantBits());
        output.writeLong(uuid.getLeastSignificantBits());
    }

    @Override
    public <I extends InputShim> UUID read(final KryoShim<I, ?> kryo, final I input, final Class<UUID> uuidClass) {
        return new UUID(input.readLong(), input.readLong());
    }

    @Override
    public boolean isImmutable() {
        return true;
    }
}
