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

package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;

import java.util.Collection;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ScopeP<V> extends P<V> {

    private static final Object EMPTY_OBJECT = new Object();

    private final String key;

    public ScopeP(final P<?> predicate) {
        super((BiPredicate) predicate.getBiPredicate(), (V) EMPTY_OBJECT);
        this.key = predicate.getValue() instanceof Collection ? ((Collection<String>) predicate.getValue()).iterator().next() : (String) predicate.getValue();   // HACK: for within("x") -- it sees that as an array
    }

    public void bind(final Scoping scopingStep, final Traverser.Admin<?> traverser) {
        this.value = (V) scopingStep.getOptionalScopeValueByKey(this.key, traverser).orElse(null);
    }

    @Override
    public boolean test(final V testValue) {
        return this.biPredicate.test(testValue, this.value);
    }

    @Override
    public ScopeP<V> negate() {
        this.biPredicate = this.biPredicate.negate();
        return this; // TODO: not cloning -- is that bad?
    }

    public String getKey() {
        return this.key;
    }

    @Override
    public String toString() {
        return this.biPredicate.toString() + "(as(" + this.key + "))";
    }
}
