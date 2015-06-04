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
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Scoping {

    public default <S> S getScopeValueByKey(final String key, final Traverser.Admin<?> traverser) throws IllegalArgumentException {
        if (traverser.getSideEffects().get(key).isPresent())
            return traverser.getSideEffects().<S>get(key).get();
        if (Scope.local == this.getScope()) {
            final S s = ((Map<String, S>) traverser.get()).get(key);
            if (null != s)
                return s;
            else
                throw new IllegalArgumentException("Neither the current map nor sideEffects have a " + key + "-key:" + this);
        } else {
            final Path path = traverser.path();
            if (path.hasLabel(key))
                return path.get(key);
            else
                throw new IllegalArgumentException("Neither the current path nor sideEffects have a " + key + "-key: " + this);
        }
    }

    public default <S> Optional<S> getOptionalScopeValueByKey(final String key, final Traverser.Admin<?> traverser) {
        if (traverser.getSideEffects().get(key).isPresent())
            return traverser.getSideEffects().<S>get(key);

        if (Scope.local == this.getScope()) {
            return Optional.ofNullable(((Map<String, S>) traverser.get()).get(key));
        } else {
            final Path path = traverser.path();
            return path.hasLabel(key) ? Optional.of(path.get(key)) : Optional.<S>empty();
        }
    }

    public Scope getScope();

    public Scope recommendNextScope();

    public void setScope(final Scope scope);

    public Set<String> getScopeKeys();

}
