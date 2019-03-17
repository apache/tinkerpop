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
package org.apache.tinkerpop.machine.coefficient;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LongCoefficient implements Coefficient<Long> {

    private Long value;

    private LongCoefficient(final Long value) {
        this.value = value;
    }

    public LongCoefficient() {
        this.value = 1L;
    }

    @Override
    public Coefficient<Long> sum(final Coefficient<Long> other) {
        this.value = this.value + other.value();
        return this;
    }

    @Override
    public Coefficient<Long> multiply(final Coefficient<Long> other) {
        this.value = this.value * other.value();
        return this;
    }

    @Override
    public Coefficient<Long> set(final Long value) {
        this.value = value;
        return this;
    }

    @Override
    public Coefficient<Long> unity() {
        this.value = 1L;
        return this;
    }

    @Override
    public Coefficient<Long> zero() {
        this.value = 0L;
        return this;
    }

    @Override
    public boolean isUnity() {
        return 1L == this.value;
    }

    @Override
    public boolean isZero() {
        return 0L == this.value;
    }

    @Override
    public Long value() {
        return this.value;
    }

    @Override
    public Long count() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public LongCoefficient clone() {
        try {
            return (LongCoefficient) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static LongCoefficient create(final Long coefficient) {
        return new LongCoefficient(coefficient);
    }

    public static LongCoefficient create() {
        return new LongCoefficient(1L);
    }
}
