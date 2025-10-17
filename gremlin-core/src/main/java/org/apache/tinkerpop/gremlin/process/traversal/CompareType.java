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
package org.apache.tinkerpop.gremlin.process.traversal;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code CompareType} is a {@code BiPredicate} that determines whether the first argument is a type of the second argument.
 *
 */
public enum CompareType implements PBiPredicate<Object, Class<?>> {

    /**
     * Evaluates if the first object is an instance of the second class.
     *
     * @since 3.8.0
     */
    typeOf {
        @Override
        public boolean test(final Object first, final Class<?> second) {
            if (first == null) {
                return second == null;
            }
            return second != null && second.isAssignableFrom(first.getClass());
        }

    };

    public static final class GlobalTypeCache {

        private GlobalTypeCache() {
            throw new IllegalStateException("Utility class");
        }

        /**
         * A register of enum names for all types.
         */
        private static final Map<String, Class<?>> GLOBAL_TYPE_REGISTRY = new ConcurrentHashMap<>();

        // register the GType classes for convinience use of simple class names in grammar
        static {
            for (GType value : GType.values()) {
                if (value == GType.NULL) {
                    // skip the null type
                    continue;
                }
                registerDataType(value.getType());
            }
        }

        /**
         * Register a type with a custom name.
         * If the type is not in the registry then the grammar cannot reference it
         */
        public static void registerDataType(final String name, final Class<?> type) {
            GLOBAL_TYPE_REGISTRY.put(name, type);
        }

        /**
         * Register a type by its simple name.
         * If the type is not in the registry then the grammar cannot reference it
         */
        public static void registerDataType(final Class<?> type) {
            GLOBAL_TYPE_REGISTRY.put(type.getSimpleName(), type);
        }

        /**
         * Unregisters a type by its name.
         * If the type is not in the registry then the grammar cannot reference it.
         */
        public static void unregisterDataType(final String name) {
            GLOBAL_TYPE_REGISTRY.remove(name);
        }

        /**
         * Looks up a Gremlin DataType by its simple name.
         */
        public static Optional<Class<?>> getRegisteredType(final String typeName) {
            if (GLOBAL_TYPE_REGISTRY.containsKey(typeName))
                return Optional.of(GLOBAL_TYPE_REGISTRY.get(typeName));

            return Optional.empty();
        }
    }
}
