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

package org.apache.tinkerpop.gremlin.structure;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class P<V> implements Predicate<V>, Serializable {

    private final BiPredicate<V, V> biPredicate;
    private final V value;

    public P(final BiPredicate<V, V> biPredicate, final V value) {
        this.value = value;
        this.biPredicate = biPredicate;
    }

    public BiPredicate<V, V> getBiPredicate() {
        return this.biPredicate;
    }

    public V getValue() {
        return this.value;
    }

    @Override
    public boolean test(final V testValue) {
        return this.biPredicate.test(testValue, this.value);
    }

    @Override
    public int hashCode() {
        return this.biPredicate.hashCode() + this.value.hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof P &&
                ((P) other).getBiPredicate().equals(this.biPredicate) &&
                ((((P) other).getValue() == null && this.getValue() == null) || ((P) other).getValue().equals(this.getValue()));
    }

    @Override
    public String toString() {
        return this.biPredicate.toString() + "(" + this.value + ")";
    }

    //////////////// statics

    public static <V> P<V> eq(final V value) {
        return new P(Compare.eq, value);
    }

    public static <V> P<V> neq(final V value) {
        return new P(Compare.neq, value);
    }

    public static <V> P<V> lt(final V value) {
        return new P(Compare.lt, value);
    }

    public static <V> P<V> lte(final V value) {
        return new P(Compare.lte, value);
    }

    public static <V> P<V> gt(final V value) {
        return new P(Compare.gt, value);
    }

    public static <V> P<V> gte(final V value) {
        return new P(Compare.gte, value);
    }

    public static <V> P<V>[] inside(final V first, final V second) {
        return new P[]{new P(Compare.gt, first), new P(Compare.lt, second)};
    }

    public static <V> P<V>[] outside(final V first, final V second) {
        return new P[]{new P(Compare.lt, first), new P(Compare.gt, second)};
    }

    public static <V> P<V>[] between(final V first, final V second) {
        return new P[]{new P(Compare.gte, first), new P(Compare.lt, second)};
    }

    public static <V> P<V> within(final V... values) {
        return new P(Contains.within, values.length == 0 ? null : Arrays.asList(values));
    }

    public static <V> P<V> within(final Collection<V> value) {
        return new P(Contains.within, value);
    }

    public static <V> P<V> without(final V... values) {
        return new P(Contains.without, values.length == 0 ? null : Arrays.asList(values));
    }

    public static <V> P<V> without(final Collection<V> value) {
        return new P(Contains.without, value);
    }

    public static P test(final BiPredicate biPredicate, final Object value) {
        return new P(biPredicate, value);
    }
}
