/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Scoping {

    public default <S> S getScopeValueByKey(final String key, final Traverser.Admin<?> traverser) throws IllegalArgumentException {
        if (Scope.local == this.getScope()) {
            final S s = ((Map<String, S>) traverser.get()).get(key);
            if (null == s)
                throw new IllegalArgumentException("The provided map does not have a " + key + "-key: " + traverser);
            return s;
        } else {
            final Path path = traverser.path();
            return path.hasLabel(key) ? path.get(key) : traverser.getSideEffects().<S>get(key).orElseThrow(() -> new IllegalArgumentException("Neither the current path nor sideEffects have a " + key + "-key: " + traverser));
        }
    }

    public default <S> Optional<S> getOptionalScopeValueByKey(final String key, final Traverser.Admin<?> traverser) {
        if (Scope.local == this.getScope()) {
            return Optional.ofNullable(((Map<String, S>) traverser.get()).get(key));
        } else {
            final Path path = traverser.path();
            return Optional.ofNullable(path.hasLabel(key) ? path.get(key) : traverser.getSideEffects().<S>get(key).orElse(null));
        }
    }

    public Scope getScope();

    public Scope recommendNextScope();

    public void setScope(final Scope scope);

}
