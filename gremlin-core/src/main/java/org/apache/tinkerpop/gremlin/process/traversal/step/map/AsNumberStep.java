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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.tinkerpop.gremlin.process.traversal.N;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.NumberHelper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Reference implementation for number parsing step.
 */
public final class AsNumberStep<S> extends ScalarMapStep<S, Number> {

    private N numberToken;

    public AsNumberStep(final Traversal.Admin traversal) {
        super(traversal);
        this.numberToken = null;
    }

    public AsNumberStep(final Traversal.Admin traversal, final N numberToken) {
        super(traversal);
        this.numberToken = numberToken;
    }

    @Override
    protected Number map(Traverser.Admin<S> traverser) {
        final Object object = traverser.get();
        if (object instanceof String) {
            String numberText = (String) object;
            Number number = parseNumber(numberText);
            return numberToken == null ? autoNumber(number) : castNumber(number, numberToken);
        } else if (object instanceof Number) {
            Number number = (Number) object;
            return numberToken == null ? autoNumber(number) : castNumber(number, numberToken);
        }
        throw new IllegalArgumentException(String.format("Can't parse type %s as number.", object == null ? "null" : object.getClass().getSimpleName()));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (numberToken != null ? numberToken.hashCode() : 0);
        return result;
    }

    @Override
    public AsNumberStep<S> clone() {
        final AsNumberStep<S> clone = (AsNumberStep<S>) super.clone();
        clone.numberToken = this.numberToken;
        return clone;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this);
    }

    public static Number parseNumber(final String value) {
        try {
            boolean isFloatingPoint = value.contains(".") || value.contains("e") || value.contains("E");
            if (isFloatingPoint) {
                BigDecimal result = new BigDecimal(value.trim());
                if (BigDecimal.valueOf(result.doubleValue()).compareTo(result) == 0) {
                    return result.doubleValue();
                }
                return result;
            }
            BigInteger result = new BigInteger(value.trim());
            if (result.bitLength() <= 31) { // default to int if not specified, smaller sizes need to be intentionally set
                return result.intValue();
            } else if (result.bitLength() <= 63) {
                return result.longValue();
            }
            return result;
        } catch (NumberFormatException nfe) {
            throw new NumberFormatException(String.format("Can't parse string '%s' as number.", value));
        }
    }

    private static Number autoNumber(Number number) {
        final Class<? extends Number> clazz = number.getClass();
        if (clazz.equals(Float.class)) {
            return castNumber(number, N.ndouble);
        }
        return number;
    }

    private static Number castNumber(Number number, N numberToken) {
        int sourceBits = getNumberBitsBasedOnValue(number);
        int targetBits = getNumberTokenBits(numberToken);
        if (sourceBits > targetBits) {
            throw new ArithmeticException(String.format("Can't convert number of type %s to %s due to overflow.",
                    number.getClass().getSimpleName(), numberToken.toString()));
        }
        if (numberToken == N.nbyte) {
            return number.byteValue();
        } else if (numberToken == N.nshort) {
            return number.shortValue();
        } else if (numberToken == N.nint) {
            return number.intValue();
        } else if (numberToken == N.nlong) {
            return number.longValue();
        } else if (numberToken == N.nfloat) {
            return number.floatValue();
        } else if (numberToken == N.ndouble) {
            return number.doubleValue();
        } else if (numberToken == N.nbigInt) {
            return BigInteger.valueOf(number.longValue());
        } else if (numberToken == N.nbigDecimal) {
            return BigDecimal.valueOf(number.doubleValue());
        }
        return number;
    }

    private static int getNumberBitsBasedOnValue(Number number) {
        final Class<? extends Number> clazz = number.getClass();
        if (clazz.equals(BigInteger.class)) {
            return 128;
        } else if (clazz.equals(BigDecimal.class)) {
            return 128;
        }
        boolean floatingPoint = (clazz.equals(Float.class) || clazz.equals(Double.class));
        if (!floatingPoint && (number.longValue() >= Byte.MIN_VALUE) && (number.longValue() <= Byte.MAX_VALUE)) {
            return 8;
        } else if (!floatingPoint && (number.longValue() >= Short.MIN_VALUE) && (number.longValue() <= Short.MAX_VALUE)) {
            return 16;
        } else if (!floatingPoint && (number.longValue() >= Integer.MIN_VALUE) && (number.longValue() <= Integer.MAX_VALUE)) {
            return 32;
        } else if (floatingPoint && (number.doubleValue() >= Float.MIN_VALUE) && (number.doubleValue() <= Float.MAX_VALUE)) {
            return 32;
        } else {
            return 64;
        }
    }

    private static int getNumberTokenBits(N numberToken) {
        if (numberToken == N.nbyte) {
            return 8;
        } else if (numberToken == N.nshort) {
            return 16;
        } else if (numberToken == N.nint) {
            return 32;
        } else if (numberToken == N.nlong) {
            return 64;
        } else if (numberToken == N.nfloat) {
            return 32;
        } else if (numberToken == N.ndouble) {
            return 64;
        }
        return 128;
    }
}
