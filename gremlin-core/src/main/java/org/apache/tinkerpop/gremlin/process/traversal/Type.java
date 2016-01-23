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

import org.slf4j.LoggerFactory;

import java.util.function.BiPredicate;

/**
 * {@link org.apache.tinkerpop.gremlin.process.traversal.Type} is a {@link java.util.function.BiPredicate} that
 * evaluates whether the first object is an instance of the class given in the second argument. If the second argument
 * is an instance of {@link java.lang.String}, then it will be handled as a class name. If it's neither an instance of
 * {@link java.lang.Class} nor an instance of {@link java.lang.String}, then the objects class will be used.
 * <p/>
 * <pre>
 * "gremlin" Type.instanceOf String == true
 * "gremlin" Type.instanceOf Number == false
 * "gremlin" Type.instanceOf "java.lang.String" == true
 * </pre>
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public enum Type implements BiPredicate<Object, Object> {

    /**
     * The first object is within the {@link java.util.Collection} provided in the second object.
     */
    instanceOf {
        @Override
        public boolean test(final Object first, final Object second) {
            try {
                return first == null || asClass(second).isInstance(first);
            } catch (ClassNotFoundException e) {
                LoggerFactory.getLogger(Type.class).error(e.getMessage());
                return false;
            }
        }
    };

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract boolean test(final Object first, final Object second);

    private static Class asClass(final Object obj) throws ClassNotFoundException {
        if (obj instanceof Class) return (Class) obj;
        if (obj instanceof String) return Class.forName((String) obj);
        return obj.getClass();
    }
}
