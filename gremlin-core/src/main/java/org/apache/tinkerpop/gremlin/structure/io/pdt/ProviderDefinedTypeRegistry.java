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

    private final Map<String, ProviderDefinedTypeAdapter<?>> adaptersByName = new ConcurrentHashMap<>();
    private final Map<Class<?>, ProviderDefinedTypeAdapter<?>> adaptersByClass = new ConcurrentHashMap<>();

    private ProviderDefinedTypeRegistry() {}

    /**
     * Creates a registry populated via {@link ServiceLoader} discovery.
     */
    @SuppressWarnings("rawtypes")
    public static ProviderDefinedTypeRegistry build() {
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

    public void register(final ProviderDefinedTypeAdapter<?> adapter) {
        adaptersByName.put(adapter.typeName(), adapter);
        adaptersByClass.put(adapter.targetClass(), adapter);
    }

    public Optional<ProviderDefinedTypeAdapter<?>> getAdapterByName(final String name) {
        return Optional.ofNullable(adaptersByName.get(name));
    }

    public Optional<ProviderDefinedTypeAdapter<?>> getAdapterByClass(final Class<?> clazz) {
        return Optional.ofNullable(adaptersByClass.get(clazz));
    }

    /**
     * Attempts to hydrate a {@link ProviderDefinedType} into a typed object using a registered adapter.
     * Recursively hydrates nested PDT values in the properties map (including those inside Lists, Sets,
     * and Maps) before calling the adapter.
     * Returns the original PDT if no adapter is found or if the adapter throws an exception.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object hydrate(final ProviderDefinedType pdt) {
        final ProviderDefinedTypeAdapter adapter = adaptersByName.get(pdt.getName());
        if (adapter == null)
            return pdt;

        // recursively hydrate nested PDTs in the properties map
        final Map<String, Object> hydrated = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : pdt.getProperties().entrySet()) {
            hydrated.put(entry.getKey(), hydrateValue(entry.getValue()));
        }

        try {
            return adapter.fromProperties(hydrated);
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
}
