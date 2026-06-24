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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for {@link ProviderDefinedTypeAdapter} instances that supports hydration of
 * {@link ProviderDefinedType} values into typed objects.
 */
public final class ProviderDefinedTypeRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ProviderDefinedTypeRegistry.class);

    private final Map<String, CompositePDTAdapter<?>> adaptersByName = new ConcurrentHashMap<>();
    private final Map<Class<?>, CompositePDTAdapter<?>> adaptersByClass = new ConcurrentHashMap<>();
    private final Map<String, PrimitivePDTAdapter<?>> primitiveAdaptersByName = new ConcurrentHashMap<>();
    private final Map<Class<?>, PrimitivePDTAdapter<?>> primitiveAdaptersByClass = new ConcurrentHashMap<>();

    private ProviderDefinedTypeRegistry() {}

    /**
     * Creates a registry populated via {@link ServiceLoader} discovery.
     */
    @SuppressWarnings("rawtypes")
    public static ProviderDefinedTypeRegistry create() {
        final ProviderDefinedTypeRegistry registry = new ProviderDefinedTypeRegistry();
        for (final ProviderDefinedTypeAdapter adapter : ServiceLoader.load(ProviderDefinedTypeAdapter.class)) {
            registry.register(adapter);
        }
        return registry;
    }

    /**
     * Creates an empty registry for manual registration.
     */
    public static ProviderDefinedTypeRegistry empty() {
        return new ProviderDefinedTypeRegistry();
    }

    /**
     * Registers an adapter. Composite adapters ({@link CompositePDTAdapter}) are stored in composite maps;
     * primitive adapters ({@link PrimitivePDTAdapter}) are stored in primitive maps.
     *
     * @throws IllegalArgumentException if the adapter's target class is already registered under the other kind
     */
    public void register(final ProviderDefinedTypeAdapter<?> adapter) {
        if (adapter instanceof PrimitivePDTAdapter) {
            final PrimitivePDTAdapter<?> primitive = (PrimitivePDTAdapter<?>) adapter;
            if (adaptersByClass.containsKey(primitive.targetClass()))
                throw new IllegalArgumentException("Class " + primitive.targetClass().getName() +
                        " is already registered as a composite PDT adapter");
            primitiveAdaptersByName.put(primitive.typeName(), primitive);
            primitiveAdaptersByClass.put(primitive.targetClass(), primitive);
        } else if (adapter instanceof CompositePDTAdapter) {
            final CompositePDTAdapter<?> composite = (CompositePDTAdapter<?>) adapter;
            if (primitiveAdaptersByClass.containsKey(composite.targetClass()))
                throw new IllegalArgumentException("Class " + composite.targetClass().getName() +
                        " is already registered as a primitive PDT adapter");
            adaptersByName.put(composite.typeName(), composite);
            adaptersByClass.put(composite.targetClass(), composite);
        }
    }

    /**
     * Registers one or more classes annotated with {@link ProviderDefined} for automatic round-trip hydration.
     * An adapter is synthesized from the annotation metadata using reflection.
     *
     * @throws IllegalArgumentException if any class is not annotated with {@link ProviderDefined}
     */
    public void register(final Class<?>... annotatedClasses) {
        for (final Class<?> clazz : annotatedClasses) {
            register(AnnotatedTypeAdapter.of(clazz));
        }
    }

    public Optional<ProviderDefinedTypeAdapter<?>> getAdapterByName(final String name) {
        return Optional.ofNullable(adaptersByName.get(name));
    }

    public Optional<ProviderDefinedTypeAdapter<?>> getAdapterByClass(final Class<?> clazz) {
        return Optional.ofNullable(adaptersByClass.get(clazz));
    }

    public Optional<PrimitivePDTAdapter<?>> getPrimitiveAdapterByName(final String name) {
        return Optional.ofNullable(primitiveAdaptersByName.get(name));
    }

    public Optional<PrimitivePDTAdapter<?>> getPrimitiveAdapterByClass(final Class<?> clazz) {
        return Optional.ofNullable(primitiveAdaptersByClass.get(clazz));
    }

    /**
     * Attempts to hydrate a {@link ProviderDefinedType} into a typed object using a registered adapter.
     * Recursively hydrates nested PDT values in the fields map (including those inside Lists, Sets,
     * and Maps) regardless of whether the outer type itself has a registered adapter — so a registered
     * inner type is hydrated even when nested inside an unregistered outer PDT.
     * Returns the original PDT (with nested values hydrated) if no adapter is found for the outer type,
     * or if the adapter throws an exception.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object hydrate(final ProviderDefinedType pdt) {
        // recursively hydrate nested PDTs in the fields map, whether or not the outer has an adapter
        boolean nestedChanged = false;
        final Map<String, Object> hydrated = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : pdt.getFields().entrySet()) {
            final Object original = entry.getValue();
            final Object value = hydrateValue(original);
            if (value != original) nestedChanged = true;
            hydrated.put(entry.getKey(), value);
        }

        final CompositePDTAdapter adapter = adaptersByName.get(pdt.getName());
        if (adapter == null) {
            // No adapter for the outer type: return it raw, but with any registered nested types hydrated.
            // Preserve identity when nothing nested was hydrated.
            return nestedChanged ? new ProviderDefinedType(pdt.getName(), hydrated) : pdt;
        }

        try {
            return adapter.fromFields(hydrated);
        } catch (final Exception e) {
            logger.warn("Failed to hydrate ProviderDefinedType '{}', returning raw PDT: {}",
                    pdt.getName(), e.getMessage());
            return pdt;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object hydrateValue(final Object value) {
        if (value instanceof ProviderDefinedType)
            return hydrate((ProviderDefinedType) value);
        if (value instanceof PrimitiveProviderDefinedType)
            return hydratePrimitive((PrimitiveProviderDefinedType) value);
        if (value instanceof List) {
            final List<Object> result = new ArrayList<>();
            for (final Object item : (List<?>) value)
                result.add(hydrateValue(item));
            return result;
        }
        if (value instanceof Set) {
            final Set<Object> result = new LinkedHashSet<>();
            for (final Object item : (Set<?>) value)
                result.add(hydrateValue(item));
            return result;
        }
        if (value instanceof Map) {
            final Map<Object, Object> result = new LinkedHashMap<>();
            for (final Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet())
                result.put(entry.getKey(), hydrateValue(entry.getValue()));
            return result;
        }
        return value;
    }

    /**
     * Attempts to hydrate a {@link PrimitiveProviderDefinedType} into a typed object using a registered
     * {@link PrimitivePDTAdapter}. Returns the original primitive PDT if no adapter is found or if the
     * adapter throws an exception.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object hydratePrimitive(final PrimitiveProviderDefinedType pdt) {
        final PrimitivePDTAdapter adapter = primitiveAdaptersByName.get(pdt.getName());
        if (adapter == null) {
            logger.warn("No PrimitivePDTAdapter registered for '{}', returning raw PrimitiveProviderDefinedType",
                    pdt.getName());
            return pdt;
        }
        try {
            return adapter.fromValue(pdt.getValue());
        } catch (final Exception e) {
            logger.warn("Failed to hydrate PrimitiveProviderDefinedType '{}', returning raw: {}",
                    pdt.getName(), e.getMessage());
            return pdt;
        }
    }

    /**
     * A reflective adapter synthesized from a {@link ProviderDefined}-annotated class.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static final class AnnotatedTypeAdapter<T> implements CompositePDTAdapter<T> {
        private final String typeName;
        private final Class<T> targetClass;
        private final Field[] fields;

        private AnnotatedTypeAdapter(final String typeName, final Class<T> targetClass, final Field[] fields) {
            this.typeName = typeName;
            this.targetClass = targetClass;
            this.fields = fields;
        }

        static <T> AnnotatedTypeAdapter<T> of(final Class<T> clazz) {
            if (!clazz.isAnnotationPresent(ProviderDefined.class))
                throw new IllegalArgumentException(clazz.getName() + " is not annotated with @ProviderDefined");
            try {
                clazz.getDeclaredConstructor();
            } catch (final NoSuchMethodException e) {
                throw new IllegalArgumentException(clazz.getName() +
                        " must have a no-arg constructor for annotation-based hydration");
            }
            // reuse ProviderDefinedType's validated, cached field/name resolution
            return new AnnotatedTypeAdapter<>(
                    ProviderDefinedType.resolveTypeName(clazz),
                    clazz,
                    ProviderDefinedType.resolveFields(clazz));
        }

        @Override public String typeName() { return typeName; }
        @Override public Class<T> targetClass() { return targetClass; }

        @Override
        public Map<String, Object> toFields(final T obj) {
            return ProviderDefinedType.from(obj).getFields();
        }

        @Override
        public T fromFields(final Map<String, Object> fieldMap) {
            try {
                final java.lang.reflect.Constructor<T> ctor = targetClass.getDeclaredConstructor();
                ctor.setAccessible(true);
                final T obj = ctor.newInstance();
                for (final Field field : fields) {
                    final Object value = fieldMap.get(field.getName());
                    if (value != null)
                        field.set(obj, coerce(value, field.getType()));
                }
                return obj;
            } catch (final ReflectiveOperationException e) {
                throw new RuntimeException("Failed to hydrate " + targetClass.getName() + ": " + e, e);
            }
        }

        private static Object coerce(final Object value, final Class<?> targetType) {
            if (targetType.isInstance(value)) return value;
            if (value instanceof Number) {
                final Number n = (Number) value;
                if (targetType == int.class || targetType == Integer.class) return n.intValue();
                if (targetType == long.class || targetType == Long.class) return n.longValue();
                if (targetType == double.class || targetType == Double.class) return n.doubleValue();
                if (targetType == float.class || targetType == Float.class) return n.floatValue();
                if (targetType == short.class || targetType == Short.class) return n.shortValue();
                if (targetType == byte.class || targetType == Byte.class) return n.byteValue();
            }
            return value;
        }
    }
}
