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

/**
 * Many {@link Step} instance can have a variable scope.
 * {@link Scope#global}: the step operates on the entire traversal.
 * {@link Scope#local}: the step operates on the current object in the step.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Scope {

    global, local;

    public Scope opposite() {
        return global.equals(this) ? local : global;
    }

    public static <S> S getScopeValueByKey(final Scope scope, final String key, final Traverser<?> traverser) {
        if (local == scope) {
            final S s = ((Map<String, S>) traverser.get()).get(key);
            if (null == s)
                throw new IllegalArgumentException("The provided map does not have a " + key + "-key: " + traverser);
            return s;
        } else {
            final Path path = traverser.path();
            return path.hasLabel(key) ? path.get(key) : traverser.sideEffects(key);
        }
    }
}
