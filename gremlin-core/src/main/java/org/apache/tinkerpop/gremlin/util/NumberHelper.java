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
package org.apache.tinkerpop.gremlin.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.BiFunction;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class NumberHelper {

    static final NumberHelper BYTE_NUMBER_HELPER = new NumberHelper(
            (a, b) -> a.byteValue() + b.byteValue(),
            (a, b) -> a.byteValue() - b.byteValue(),
            (a, b) -> a.byteValue() * b.byteValue(),
            (a, b) -> a.byteValue() / b.byteValue(),
            (a, b) -> {
                final byte x = a.byteValue(), y = b.byteValue();
                return x <= y ? x : y;
            },
            (a, b) -> {
                final byte x = a.byteValue(), y = b.byteValue();
                return x >= y ? x : y;
            });
    static final NumberHelper SHORT_NUMBER_HELPER = new NumberHelper(
            (a, b) -> a.shortValue() + b.shortValue(),
            (a, b) -> a.shortValue() - b.shortValue(),
            (a, b) -> a.shortValue() * b.shortValue(),
            (a, b) -> a.shortValue() / b.shortValue(),
            (a, b) -> {
                final short x = a.shortValue(), y = b.shortValue();
                return x <= y ? x : y;
            },
            (a, b) -> {
                final short x = a.shortValue(), y = b.shortValue();
                return x >= y ? x : y;
            });
    static final NumberHelper INTEGER_NUMBER_HELPER = new NumberHelper(
            (a, b) -> a.intValue() + b.intValue(),
            (a, b) -> a.intValue() - b.intValue(),
            (a, b) -> a.intValue() * b.intValue(),
            (a, b) -> a.intValue() / b.intValue(),
            (a, b) -> {
                final int x = a.intValue(), y = b.intValue();
                return x <= y ? x : y;
            },
            (a, b) -> {
                final int x = a.intValue(), y = b.intValue();
                return x >= y ? x : y;
            });
    static final NumberHelper LONG_NUMBER_HELPER = new NumberHelper(
            (a, b) -> a.longValue() + b.longValue(),
            (a, b) -> a.longValue() - b.longValue(),
            (a, b) -> a.longValue() * b.longValue(),
            (a, b) -> a.longValue() / b.longValue(),
            (a, b) -> {
                final long x = a.longValue(), y = b.longValue();
                return x <= y ? x : y;
            },
            (a, b) -> {
                final long x = a.longValue(), y = b.longValue();
                return x >= y ? x : y;
            });
    static final NumberHelper BIG_INTEGER_NUMBER_HELPER = new NumberHelper(
            (a, b) -> bigIntegerValue(a).add(bigIntegerValue(b)),
            (a, b) -> bigIntegerValue(a).subtract(bigIntegerValue(b)),
            (a, b) -> bigIntegerValue(a).multiply(bigIntegerValue(b)),
            (a, b) -> bigIntegerValue(a).divide(bigIntegerValue(b)),
            (a, b) -> {
                final BigInteger x = bigIntegerValue(a), y = bigIntegerValue(b);
                return x.compareTo(y) <= 0 ? x : y;
            },
            (a, b) -> {
                final BigInteger x = bigIntegerValue(a), y = bigIntegerValue(b);
                return x.compareTo(y) >= 0 ? x : y;
            });
    static final NumberHelper FLOAT_NUMBER_HELPER = new NumberHelper(
            (a, b) -> a.floatValue() + b.floatValue(),
            (a, b) -> a.floatValue() - b.floatValue(),
            (a, b) -> a.floatValue() * b.floatValue(),
            (a, b) -> a.floatValue() / b.floatValue(),
            (a, b) -> {
                final float x = a.floatValue(), y = b.floatValue();
                return x <= y ? x : y;
            },
            (a, b) -> {
                final float x = a.floatValue(), y = b.floatValue();
                return x >= y ? x : y;
            });
    static final NumberHelper DOUBLE_NUMBER_HELPER = new NumberHelper(
            (a, b) -> a.doubleValue() + b.doubleValue(),
            (a, b) -> a.doubleValue() - b.doubleValue(),
            (a, b) -> a.doubleValue() * b.doubleValue(),
            (a, b) -> a.doubleValue() / b.doubleValue(),
            (a, b) -> {
                final double x = a.doubleValue(), y = b.doubleValue();
                return x <= y ? x : y;
            },
            (a, b) -> {
                final double x = a.doubleValue(), y = b.doubleValue();
                return x >= y ? x : y;
            });
    static final NumberHelper BIG_DECIMAL_NUMBER_HELPER = new NumberHelper(
            (a, b) -> bigDecimalValue(a).add(bigDecimalValue(b)),
            (a, b) -> bigDecimalValue(a).subtract(bigDecimalValue(b)),
            (a, b) -> bigDecimalValue(a).multiply(bigDecimalValue(b)),
            (a, b) -> bigDecimalValue(a).divide(bigDecimalValue(b)),
            (a, b) -> {
                final BigDecimal x = bigDecimalValue(a), y = bigDecimalValue(b);
                return x.compareTo(y) <= 0 ? x : y;
            },
            (a, b) -> {
                final BigDecimal x = bigDecimalValue(a), y = bigDecimalValue(b);
                return x.compareTo(y) >= 0 ? x : y;
            });
    public final BiFunction<Number, Number, Number> add;
    public final BiFunction<Number, Number, Number> sub;
    public final BiFunction<Number, Number, Number> mul;
    public final BiFunction<Number, Number, Number> div;
    public final BiFunction<Number, Number, Number> min;
    public final BiFunction<Number, Number, Number> max;

    private NumberHelper(final BiFunction<Number, Number, Number> add,
                         final BiFunction<Number, Number, Number> sub,
                         final BiFunction<Number, Number, Number> mul,
                         final BiFunction<Number, Number, Number> div,
                         final BiFunction<Number, Number, Number> min,
                         final BiFunction<Number, Number, Number> max
    ) {
        this.add = add;
        this.sub = sub;
        this.mul = mul;
        this.div = div;
        this.min = min;
        this.max = max;
    }

    public static Class<? extends Number> getHighestCommonNumberClass(final Number... numbers) {
        return getHighestCommonNumberClass(false, numbers);
    }

    public static Class<? extends Number> getHighestCommonNumberClass(final boolean forceFloatingPoint, final Number... numbers) {
        int bits = 8;
        boolean fp = forceFloatingPoint;
        for (final Number number : numbers) {
            if (number == null) continue;
            final Class<? extends Number> clazz = number.getClass();
            if (clazz.equals(Byte.class)) continue;
            if (clazz.equals(Short.class)) {
                bits = bits < 16 ? 16 : bits;
            } else if (clazz.equals(Integer.class)) {
                bits = bits < 32 ? 32 : bits;
            } else if (clazz.equals(Long.class)) {
                bits = bits < 64 ? 64 : bits;
            } else if (clazz.equals(BigInteger.class)) {
                bits = bits < 128 ? 128 : bits;
            } else if (clazz.equals(Float.class)) {
                bits = bits < 32 ? 32 : bits;
                fp = true;
            } else if (clazz.equals(Double.class)) {
                bits = bits < 64 ? 64 : bits;
                fp = true;
            } else /*if (clazz.equals(BigDecimal.class))*/ {
                bits = bits < 128 ? 128 : bits;
                fp = true;
                break; // maxed out, no need to check remaining numbers
            }
        }
        return determineNumberClass(bits, fp);
    }

    public static Number add(final Number a, final Number b) {
        final Class<? extends Number> clazz = getHighestCommonNumberClass(a, b);
        return getHelper(clazz).add.apply(a, b);
    }

    public static Number sub(final Number a, final Number b) {
        final Class<? extends Number> clazz = getHighestCommonNumberClass(a, b);
        return getHelper(clazz).sub.apply(a, b);
    }

    public static Number mul(final Number a, final Number b) {
        final Class<? extends Number> clazz = getHighestCommonNumberClass(a, b);
        return getHelper(clazz).mul.apply(a, b);
    }

    public static Number div(final Number a, final Number b) {
        return div(a, b, false);
    }

    public static Number div(final Number a, final Number b, final boolean forceFloatingPoint) {
        final Class<? extends Number> clazz = getHighestCommonNumberClass(forceFloatingPoint, a, b);
        return getHelper(clazz).div.apply(a, b);
    }

    public static Number min(final Number a, final Number b) {
        final Class<? extends Number> clazz = getHighestCommonNumberClass(a, b);
        return getHelper(clazz).min.apply(a, b);
    }

    public static Number max(final Number a, final Number b) {
        final Class<? extends Number> clazz = getHighestCommonNumberClass(a, b);
        return getHelper(clazz).max.apply(a, b);
    }

    private static NumberHelper getHelper(final Class<? extends Number> clazz) {
        if (clazz.equals(Byte.class)) {
            return BYTE_NUMBER_HELPER;
        }
        if (clazz.equals(Short.class)) {
            return SHORT_NUMBER_HELPER;
        }
        if (clazz.equals(Integer.class)) {
            return INTEGER_NUMBER_HELPER;
        }
        if (clazz.equals(Long.class)) {
            return LONG_NUMBER_HELPER;
        }
        if (clazz.equals(BigInteger.class)) {
            return BIG_INTEGER_NUMBER_HELPER;
        }
        if (clazz.equals(Float.class)) {
            return FLOAT_NUMBER_HELPER;
        }
        if (clazz.equals(Double.class)) {
            return DOUBLE_NUMBER_HELPER;
        }
        if (clazz.equals(BigDecimal.class)) {
            return BIG_DECIMAL_NUMBER_HELPER;
        }
        throw new IllegalArgumentException("Unsupported numeric type: " + clazz);
    }

    private static BigInteger bigIntegerValue(final Number number) {
        if (number == null) return null;
        if (number instanceof BigInteger) return (BigInteger) number;
        return BigInteger.valueOf(number.longValue());
    }

    private static BigDecimal bigDecimalValue(final Number number) {
        if (number == null) return null;
        if (number instanceof BigDecimal) return (BigDecimal) number;
        if (number instanceof BigInteger) return new BigDecimal((BigInteger) number);
        return (number instanceof Double || number instanceof Float)
                ? BigDecimal.valueOf(number.doubleValue())
                : BigDecimal.valueOf(number.longValue());
    }

    private static Class<? extends Number> determineNumberClass(final int bits, final boolean floatingPoint) {
        if (floatingPoint) {
            if (bits <= 32) return Float.class;
            if (bits <= 64) return Double.class;
            return BigDecimal.class;
        } else {
            if (bits <= 8) return Byte.class;
            if (bits <= 16) return Short.class;
            if (bits <= 32) return Integer.class;
            if (bits <= 64) return Long.class;
            return BigInteger.class;
        }
    }
}
