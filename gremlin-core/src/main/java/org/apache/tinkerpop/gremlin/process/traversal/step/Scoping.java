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
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Scoping {

    public static enum Variable {START, END}

    public default <S> S getScopeValueByKey(final Pop pop, final String key, final Traverser.Admin<?> traverser) throws IllegalArgumentException {
        if (traverser.getSideEffects().get(key).isPresent())
            return traverser.getSideEffects().<S>get(key).get();
        if (Scope.local == this.getScope()) {
            try {
                Object object = traverser.get();
                if (!(object instanceof Map))
                    object = Collections.emptyMap();
                final S s = ((Map<String, S>) object).get(key);
                if (null != s)
                    return s;
                else
                    throw new IllegalArgumentException("Neither the current map nor sideEffects have a " + key + "-key:" + this);
            } catch (final ClassCastException e) {
                throw new IllegalStateException("The current step was compiled to an invalid local scope and this is a compilation error. Please report the full traversal that yielded this exception: " + this);
            }
        } else {
            final Path path = traverser.path();
            if (path.hasLabel(key))
                return null == pop ? path.get(key) : path.get(pop, key);
            else
                throw new IllegalArgumentException("Neither the current path nor sideEffects have a " + key + "-key: " + this);
        }
    }

    public default <S> Optional<S> getOptionalScopeValueByKey(final Pop pop, final String key, final Traverser.Admin<?> traverser) {
        if (traverser.getSideEffects().get(key).isPresent())
            return traverser.getSideEffects().<S>get(key);

        if (Scope.local == this.getScope()) {
                Object object = traverser.get();
                if (!(object instanceof Map))
                    object = Collections.emptyMap();
                return Optional.ofNullable(((Map<String, S>) object).get(key));
        } else {
            final Path path = traverser.path();
            return path.hasLabel(key) ? Optional.of(null == pop ? path.get(key) : path.get(pop, key)) : Optional.<S>empty();
        }
    }

    public Scope getScope();

    public Scope recommendNextScope();

    public void setScope(final Scope scope);

    public Set<String> getScopeKeys();
}
