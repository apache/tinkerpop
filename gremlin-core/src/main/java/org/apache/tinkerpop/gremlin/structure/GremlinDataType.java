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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public interface GremlinDataType {

    String getName();
    Class<?> getType();
    GremlinDataType fromName(String name);

    // Optional: provide default categorization methods
    default boolean isPrimitive() {
        return getType().isPrimitive();
    }

    default boolean isNumeric() {
        Class<?> type = getType();
        return type == int.class || type == Integer.class ||
                type == long.class || type == Long.class ||
                type == double.class || type == Double.class ||
                type == float.class || type == Float.class ||
                type == BigDecimal.class || type == BigInteger.class;
    }

    final class GlobalTypeCache {

        private GlobalTypeCache() {
            throw new IllegalStateException("Utility class");
        }

        /**
         * A register of the simple names for all strategies.
         */
        private static final Map<String, GremlinDataType> GLOBAL_TYPE_REGISTRY = new ConcurrentHashMap<>() {};

        static {
            for (GremlinDataType value : GType.values()) {
                GLOBAL_TYPE_REGISTRY.put("GType." + value.getName(), value);
            }
        }

        /**
         * Registers all constants of a GremlinDataType class by its name (formatted as {GremlinDataType}.{TYPE}), so
         * it is available to the grammar when parsing Gremlin.
         */
        public static void registerDataType(final Class<? extends Enum<? extends GremlinDataType>> enumClass) {
            for (Enum<? extends GremlinDataType> constant : enumClass.getEnumConstants()) {
                GLOBAL_TYPE_REGISTRY.put(enumClass.getSimpleName() + "." + constant.name(), (GremlinDataType) constant);
            }
        }

        /**
         * Unregisters a single GremlinDataType Enum constant by its name (formatted as {GremlinDataType}.{TYPE}).
         * If the GremlinDataType is not in the registry then the grammar cannot reference it
         */
        public static void registerDataType(final String name, final GremlinDataType gdt) {
            GLOBAL_TYPE_REGISTRY.put(name, gdt);
        }

        /**
         * Unregisters all constants of a GremlinDataType class. If the GremlinDataType is not in the registry then the
         * grammar cannot reference it
         */
        public static void unregisterDataType(final Class<? extends Enum<? extends GremlinDataType>> enumClass) {
            for (Enum<? extends GremlinDataType> constant : enumClass.getEnumConstants()) {
                GLOBAL_TYPE_REGISTRY.remove(enumClass.getSimpleName() + "." + constant.name());
            }
        }

        /**
         * Unregisters a single GremlinDataType by its name (formatted as {GremlinDataType}.{TYPE}). If the GremlinDataType
         * is not in the registry then the grammar cannot reference it
         */
        public static void unregisterDataType(final String name) {
            GLOBAL_TYPE_REGISTRY.remove(name);
        }

        /**
         * Looks up a Gremlin DataType by its simple name.
         */
        public static Optional<GremlinDataType> getRegisteredType(final String typeName) {
            System.out.println(GLOBAL_TYPE_REGISTRY);
            if (GLOBAL_TYPE_REGISTRY.containsKey(typeName))
                return Optional.of(GLOBAL_TYPE_REGISTRY.get(typeName));

            return Optional.empty();
        }

    }

}
