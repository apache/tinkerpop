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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An immutable representation of a provider-defined type consisting of a name and a map of fields.
 */
public final class ProviderDefinedType {

    private static final ConcurrentHashMap<Class<?>, FieldCache> FIELD_CACHE = new ConcurrentHashMap<>();

    private final String name;
    private final Map<String, Object> fields;
    private Object hydrated;

    public ProviderDefinedType(final String name, final Map<String, Object> fields) {
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("name cannot be null or empty");
        if (fields == null)
            throw new IllegalArgumentException("fields cannot be null");
        this.name = name;
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    /**
     * Creates a {@code ProviderDefinedType} from an object annotated with {@link ProviderDefined}.
     */
    public static ProviderDefinedType from(final Object obj) {
        if (obj == null)
            throw new IllegalArgumentException("obj cannot be null");

        final Class<?> clazz = obj.getClass();
        final FieldCache cache = FIELD_CACHE.computeIfAbsent(clazz, ProviderDefinedType::buildCache);

        final Map<String, Object> fields = new LinkedHashMap<>();
        for (final Field field : cache.fields) {
            try {
                fields.put(field.getName(), field.get(obj));
            } catch (Exception e) {
                throw new RuntimeException("Failed to read field '" + field.getName() + "' from " + clazz.getName(), e);
            }
        }

        return new ProviderDefinedType(cache.name, fields);
    }

    /**
     * Package-private access to the resolved type name for a {@link ProviderDefined}-annotated class.
     * Validates the annotation and field configuration via the shared field cache.
     */
    static String resolveTypeName(final Class<?> clazz) {
        return FIELD_CACHE.computeIfAbsent(clazz, ProviderDefinedType::buildCache).name;
    }

    /**
     * Package-private access to the resolved serializable fields for a {@link ProviderDefined}-annotated class.
     */
    static Field[] resolveFields(final Class<?> clazz) {
        return FIELD_CACHE.computeIfAbsent(clazz, ProviderDefinedType::buildCache).fields;
    }

    private static FieldCache buildCache(final Class<?> clazz) {
        final ProviderDefined annotation = clazz.getAnnotation(ProviderDefined.class);
        if (annotation == null)
            throw new IllegalArgumentException(clazz.getName() + " is not annotated with @ProviderDefined");

        final String typeName = annotation.name().isEmpty() ? clazz.getSimpleName() : annotation.name();
        final String[] included = annotation.includedFields();
        final String[] excluded = annotation.excludedFields();

        if (included.length > 0 && excluded.length > 0)
            throw new IllegalArgumentException("@ProviderDefined cannot specify both includedFields and excludedFields");

        final Set<String> includedSet = included.length > 0 ? new HashSet<>(Arrays.asList(included)) : null;
        final Set<String> excludedSet = excluded.length > 0 ? new HashSet<>(Arrays.asList(excluded)) : Collections.emptySet();

        final Field[] allFields = getAllFields(clazz).toArray(new Field[0]);
        final Field[] filtered = Arrays.stream(allFields)
                .filter(f -> !f.isSynthetic())
                .filter(f -> {
                    if (includedSet != null) return includedSet.contains(f.getName());
                    return !excludedSet.contains(f.getName());
                })
                .peek(f -> f.setAccessible(true))
                .toArray(Field[]::new);

        return new FieldCache(typeName, filtered);
    }

    private static List<Field> getAllFields(final Class<?> clazz) {
        final List<Field> fields = new ArrayList<>();
        Class<?> current = clazz;
        while (current != null && current != Object.class) {
            fields.addAll(Arrays.asList(current.getDeclaredFields()));
            current = current.getSuperclass();
        }
        return fields;
    }

    private static class FieldCache {
        final String name;
        final Field[] fields;

        FieldCache(final String name, final Field[] fields) {
            this.name = name;
            this.fields = fields;
        }
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    /**
     * Returns a copy of this PDT with the hydrated object attached.
     */
    public ProviderDefinedType withHydrated(final Object hydrated) {
        final ProviderDefinedType copy = new ProviderDefinedType(this.name, this.fields);
        copy.hydrated = hydrated;
        return copy;
    }

    /**
     * Returns the hydrated object if this PDT was hydrated by a {@link ProviderDefinedTypeRegistry}, or {@code null}.
     */
    public Object getHydrated() {
        return hydrated;
    }

    /**
     * Equality is based solely on {@code name} and {@code fields} (the serialized wire form).
     * The {@code hydrated} field is intentionally excluded — it is a transient, derived view
     * cached by the deserializer via {@link #withHydrated(Object)} and is not part of the
     * type's logical identity.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof ProviderDefinedType)) return false;
        final ProviderDefinedType that = (ProviderDefinedType) o;
        return name.equals(that.name) && fields.equals(that.fields);
    }

    /** See {@link #equals(Object)} for rationale on field inclusion. */
    @Override
    public int hashCode() {
        return Objects.hash(name, fields);
    }

    @Override
    public String toString() {
        return "pdt[" + name + "]" + fields;
    }
}
