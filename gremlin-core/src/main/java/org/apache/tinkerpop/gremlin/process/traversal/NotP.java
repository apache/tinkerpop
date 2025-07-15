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

import java.io.Serializable;
import java.util.Objects;

public class NotP<V> extends P<V> {
    private P<V> originalP;
    public NotP(final P<V> p) {
        super(null, (V) null);

        if (null == p) {
            throw new IllegalArgumentException("Cannot negate a null P");
        }
        this.originalP = p;
        this.biPredicate = new NotPBiPredicate<>(p.getBiPredicate());
    }

    /**
     * Gets the current value to be passed to the predicate for testing.
     */
    @Override
    public V getValue() {
        return originalP.getValue();
    }

    @Override
    public void setValue(final V value) {
        super.setValue(value);
        if (originalP != null) {
            originalP.setValue(value);
        }
    }

    @Override
    public String toString() {
        return null == this.getValue() ? this.biPredicate.toString() : String.format("not(%s(%s))", this.originalP.biPredicate.toString(), this.getValue());
    }

    @Override
    public P<V> negate() {
        return originalP;
    }

    public P<V> clone() {
        return new NotP<>(this.originalP.clone());
    }

    public static class NotPBiPredicate<T, U> implements PBiPredicate<T, U>, Serializable {
        PBiPredicate<T, U> original;

        public NotPBiPredicate(PBiPredicate<T, U> predicate) {
            this.original = predicate;
        }

        @Override
        public boolean test(T t, U u) {
            return !original.test(t, u);
        }

        @Override
        public PBiPredicate<T, U> negate() {
            return original;
        }

        @Override
        public String getPredicateName() {
            return "not";
        }

        public PBiPredicate<T, U> getOriginal() {
            return original;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NotPBiPredicate<?, ?> that = (NotPBiPredicate<?, ?>) o;
            return Objects.equals(original, that.original);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(original);
        }

        @Override
        public String toString() {
            return String.format("not(%s)", original.toString());
        }
    }
}
