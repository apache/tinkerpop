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
package org.apache.tinkerpop.gremlin.server.pdt;

import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitivePDTAdapter;

/**
 * Adapter for {@link Uint32} primitive PDT. Serializes as the unsigned decimal string representation.
 */
public final class Uint32Adapter implements PrimitivePDTAdapter<Uint32> {

    @Override
    public String typeName() {
        return "Uint32";
    }

    @Override
    public Class<Uint32> targetClass() {
        return Uint32.class;
    }

    @Override
    public String toValue(final Uint32 obj) {
        return Long.toUnsignedString(obj.getValue());
    }

    @Override
    public Uint32 fromValue(final String value) {
        final long parsed = Long.parseUnsignedLong(value);
        if (Long.compareUnsigned(parsed, 4294967295L) > 0)
            throw new IllegalArgumentException("Value exceeds Uint32 range: " + value);
        return new Uint32(parsed);
    }
}
