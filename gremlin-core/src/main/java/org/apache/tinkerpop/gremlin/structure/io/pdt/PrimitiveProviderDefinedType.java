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
package org.apache.tinkerpop.gremlin.structure.io.pdt;

import java.util.Objects;

/**
 * An immutable representation of a primitive provider-defined type consisting of a name and an opaque string value.
 */
public final class PrimitiveProviderDefinedType {

    private final String name;
    private final String value;
    private Object hydrated;

    public PrimitiveProviderDefinedType(final String name, final String value) {
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("name cannot be null or empty");
        if (value == null)
            throw new IllegalArgumentException("value cannot be null");
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    /**
     * Returns a copy of this primitive PDT with the hydrated object attached.
     */
    public PrimitiveProviderDefinedType withHydrated(final Object hydrated) {
        final PrimitiveProviderDefinedType copy = new PrimitiveProviderDefinedType(this.name, this.value);
        copy.hydrated = hydrated;
        return copy;
    }

    /**
     * Returns the hydrated object if set, or {@code null}.
     */
    public Object getHydrated() {
        return hydrated;
    }

    /**
     * Equality is based solely on {@code name} and {@code value} (the serialized wire form).
     * The {@code hydrated} field is intentionally excluded — it is a transient, derived view
     * cached by the deserializer via {@link #withHydrated(Object)} and is not part of the
     * type's logical identity.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof PrimitiveProviderDefinedType)) return false;
        final PrimitiveProviderDefinedType that = (PrimitiveProviderDefinedType) o;
        return name.equals(that.name) && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return "pdt[" + name + "](" + value + ")";
    }
}
