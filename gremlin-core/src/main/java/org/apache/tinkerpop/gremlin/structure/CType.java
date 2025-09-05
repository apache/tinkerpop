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

package org.apache.tinkerpop.gremlin.structure;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public enum CType implements GremlinDataType{
    ;

    // Registry for custom type mappings
    private static final ConcurrentMap<String, CustomTypeInfo> CUSTOM_REGISTRY = new ConcurrentHashMap<>();

    // Instance fields for the enum
    private final Class<?> javaType;

    CType(Class<?> javaType) {
        this.javaType = javaType;
    }


    @Override
    public String getName() {
        return "";
    }

    @Override
    public Class<?> getType() {
        return null;
    }

    @Override
    public GremlinDataType fromName(String name) {
        return null;
    }


    /**
     * Register a custom TypeToken enum class
     */
    public static <E extends Enum<E> & GremlinDataType> void register(
            String customTypeName,
            Class<E> enumClass,
            Function<String, E> factory) {

        CUSTOM_REGISTRY.put(customTypeName, new CustomTypeInfo(enumClass, factory));
    }

    /**
     * Register using reflection (convenience method)
     */
    public static <E extends Enum<E> & GremlinDataType> void register(
            String customTypeName,
            Class<E> enumClass) {

        register(customTypeName, enumClass, name -> Enum.valueOf(enumClass, name));
    }

    /**
     * Create a TypeToken instance from a custom type registration
     */
    public static Optional<GremlinDataType> createCustomInstance(String customTypeName, String enumValueName) {
        CustomTypeInfo info = CUSTOM_REGISTRY.get(customTypeName);
        if (info == null) {
            return Optional.empty();
        }

        try {
            return Optional.of(info.factory.apply(enumValueName));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /**
     * Get the custom type name for a TypeToken instance
     */
    public static Optional<String> getCustomTypeName(GremlinDataType token) {
        Class<?> tokenClass = token.getClass();
        return CUSTOM_REGISTRY.entrySet().stream()
                .filter(entry -> entry.getValue().enumClass.equals(tokenClass))
                .map(Map.Entry::getKey)
                .findFirst();
    }

    /**
     * Check if a custom type is registered
     */
    public static boolean isCustomTypeRegistered(String customTypeName) {
        return CUSTOM_REGISTRY.containsKey(customTypeName);
    }

    /**
     * Get all registered custom type names
     */
    public static Set<String> getRegisteredCustomTypes() {
        return Collections.unmodifiableSet(CUSTOM_REGISTRY.keySet());
    }

    /**
     * Check if a TypeToken instance is a custom type
     */
    public static boolean isCustomType(GremlinDataType token) {
        return getCustomTypeName(token).isPresent();
    }

    // Internal class to hold registration info
    private static class CustomTypeInfo {
        final Class<? extends Enum<? extends GremlinDataType>> enumClass;
        final Function<String, ? extends GremlinDataType> factory;

        CustomTypeInfo(Class<? extends Enum<? extends GremlinDataType>> enumClass,
                       Function<String, ? extends GremlinDataType> factory) {
            this.enumClass = enumClass;
            this.factory = factory;
        }
    }
}
