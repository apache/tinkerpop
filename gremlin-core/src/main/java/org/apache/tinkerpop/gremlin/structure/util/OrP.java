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

package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.tinkerpop.gremlin.structure.P;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrP<V> extends ConjunctionP<V> {

    public OrP(final P<V> predicate, final P<V>... predicates) {
        super(predicate, predicates);
    }

    @Override
    public boolean test(final V v) {
        for (final P<V> predicate : this.predicates) {
            if (predicate.test(v))
                return true;
        }
        return false;
    }

    @Override
    public P<V> or(final Predicate<? super V> predicate) {
        if (!(predicate instanceof P))
            throw new IllegalArgumentException("Only P predicates can be or'd together");
        this.predicates.add((P<V>) predicate);   // TODO: clone and add?
        return this;
    }

    @Override
    public String toString() {
        return "or(" + this.predicates + ")";
    }
}