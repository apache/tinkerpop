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

package org.apache.tinkerpop.gremlin.process.computer;

import org.apache.tinkerpop.gremlin.process.computer.util.MemoryHelper;

import java.io.Serializable;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MemoryComputeKey<A> implements Serializable {

    private final String key;
    private final BinaryOperator<A> reducer;
    private final boolean isTransient;

    private MemoryComputeKey(final String key, final BinaryOperator<A> reducer, final boolean isTransient) {
        this.key = key;
        this.reducer = reducer;
        this.isTransient = isTransient;
        MemoryHelper.validateKey(key);
    }

    public String getKey() {
        return this.key;
    }

    public boolean isTransient() {
        return this.isTransient;
    }

    public BinaryOperator<A> getReducer() {
        return this.reducer;
    }

    @Override
    public int hashCode() {
        return this.key.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof MemoryComputeKey && ((MemoryComputeKey) object).key.equals(this.key);
    }

    public static <A> MemoryComputeKey of(final String key, final BinaryOperator<A> reducer, final boolean isTransient) {
        return new MemoryComputeKey<>(key, reducer, isTransient);
    }

    public static SetOperator setOperator() {
        return SetOperator.INSTANCE;
    }

    public static AndOperator andOperator() {
        return AndOperator.INSTANCE;
    }

    public static OrOperator orOperator() {
        return OrOperator.INSTANCE;
    }

    public static SumLongOperator sumLongOperator() {
        return SumLongOperator.INSTANCE;
    }

    public static SumIntegerOperator sumIntegerOperator() {
        return SumIntegerOperator.INSTANCE;
    }

    ///////////

    public static class SetOperator implements BinaryOperator<Object>, Serializable {

        private static final SetOperator INSTANCE = new SetOperator();

        private SetOperator() {

        }

        @Override
        public Object apply(final Object first, final Object second) {
            return second;
        }
    }

    public static class AndOperator implements BinaryOperator<Boolean>, Serializable {

        private static final AndOperator INSTANCE = new AndOperator();

        private AndOperator() {

        }

        @Override
        public Boolean apply(final Boolean first, final Boolean second) {
            return first && second;
        }
    }

    public static class OrOperator implements BinaryOperator<Boolean>, Serializable {

        private static final OrOperator INSTANCE = new OrOperator();

        private OrOperator() {

        }

        @Override
        public Boolean apply(final Boolean first, final Boolean second) {
            return first || second;
        }
    }

    public static class SumLongOperator implements BinaryOperator<Long>, Serializable {

        private static final SumLongOperator INSTANCE = new SumLongOperator();

        private SumLongOperator() {

        }

        @Override
        public Long apply(final Long first, final Long second) {
            return first + second;
        }
    }

    public static class SumIntegerOperator implements BinaryOperator<Integer>, Serializable {

        private static final SumIntegerOperator INSTANCE = new SumIntegerOperator();

        private SumIntegerOperator() {

        }

        @Override
        public Integer apply(final Integer first, final Integer second) {
            return first + second;
        }
    }
}
