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

import org.apache.tinkerpop.gremlin.structure.GremlinDataType;

import java.util.Optional;

/**
 * {@code Type} is a {@code BiPredicate} that determines whether the first argument is a type of the second argument.
 *
 */
public enum Type implements PBiPredicate<Object, Object> {

    /**
     * Evaluates if the first object is equal to the second per Gremlin Comparison semantics.
     *
     * @since 3.8.0
     */
    typeOf {
        @Override
        public boolean test(final Object first, final Object second) {
            Class<?> secondClass;
            if (second instanceof GremlinDataType) {
                secondClass = ((GremlinDataType) second).getType();
            } else if (second instanceof String) {
                try {
                    // for java class names
                    secondClass = Class.forName((String) second);
                } catch (ClassNotFoundException e) {
                    // for string name of type token we can use the cache
                    final Optional<GremlinDataType> opt = GremlinDataType.GlobalTypeCache.getRegisteredType((String) second);
                    if (opt.isEmpty())
                        return false;
                    else
                        secondClass = opt.get().getType();
                }
            } else if (second instanceof Class) {
                secondClass = (Class<?>) second;
            } else {
                return false;
            }

            return (secondClass).isAssignableFrom(first.getClass());
        }

    },
}
