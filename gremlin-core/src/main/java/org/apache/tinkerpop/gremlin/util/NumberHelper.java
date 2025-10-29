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

import org.apache.tinkerpop.gremlin.process.traversal.GType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.function.Function;
import java.util.function.BiFunction;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class NumberHelper {

    static final class NumberInfo {

        private int bits;
        private final boolean fp;

        public int getBits() {
            return bits;
        }

        public boolean getFp() {
            return fp;
        }

        public void promoteBits() {
            bits <<= 1;
        }

        NumberInfo(int bits, boolean fp) {
            this.bits = bits;
            this.fp = fp;
        }
    }

    private static byte asByte(int arg) {
        if (arg > Byte.MAX_VALUE || arg < Byte.MIN_VALUE)
            throw new ArithmeticException("byte overflow");
        return (byte) arg;
    }

    private static short asShort(int arg) {
        if (arg > Short.MAX_VALUE || arg < Short.MIN_VALUE)
            throw new ArithmeticException("short overflow");
        return (short) arg;
    }

    static final NumberHelper BYTE_NUMBER_HELPER = new NumberHelper(
            (a, b) -> asByte(a.byteValue() + b.byteValue()),
            (a, b) -> asByte(a.byteValue() - b.byteValue()),
            (a, b) -> asByte(a.byteValue() * b.byteValue()),
            (a, b) -> {
                if (a.byteValue() == Byte.MIN_VALUE && b.byteValue() == -1) {
                    throw new ArithmeticException("byte overflow");
                }
                return (byte)(a.byteValue() / b.byteValue());
            },
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final byte x = a.byteValue(), y = b.byteValue();
                        return x <= y ? x : y;
                    }
                    return a.byteValue();
                }
                return b.byteValue();
            },
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final byte x = a.byteValue(), y = b.byteValue();
                        return x >= y ? x : y;
                    }
                    return a.byteValue();
                }
                return b.byteValue();
            },
            (a, b) -> Byte.compare(a.byteValue(), b.byteValue()));

    static final NumberHelper SHORT_NUMBER_HELPER = new NumberHelper(
            (a, b) -> asShort(a.shortValue() + b.shortValue()),
            (a, b) -> asShort(a.shortValue() - b.shortValue()),
            (a, b) -> asShort(a.shortValue() * b.shortValue()),
            (a, b) -> {
                if (a.shortValue() == Short.MIN_VALUE && b.shortValue() == -1) {
                    throw new ArithmeticException("short overflow");
                }
                return (short)(a.shortValue() / b.shortValue());
            },
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final short x = a.shortValue(), y = b.shortValue();
                        return x <= y ? x : y;
                    }
                    return a.shortValue();
                }
                return b.shortValue();
            },
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final short x = a.shortValue(), y = b.shortValue();
                        return x >= y ? x : y;
                    }
                    return a.shortValue();
                }
                return b.shortValue();
            },
            (a, b) -> Short.compare(a.shortValue(), b.shortValue()));

    static final NumberHelper INTEGER_NUMBER_HELPER = new NumberHelper(
            (a, b) -> Math.addExact(a.intValue(), b.intValue()),
            (a, b) -> Math.subtractExact(a.intValue(), b.intValue()),
            (a, b) -> Math.multiplyExact(a.intValue(), b.intValue()),
            (a, b) -> {
                if (a.intValue() == Integer.MIN_VALUE && b.intValue() == -1) {
                    throw new ArithmeticException("integer overflow");
                }
                return a.intValue() / b.intValue();
            },
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final int x = a.intValue(), y = b.intValue();
                        return x <= y ? x : y;
                    }
                    return a.intValue();
                }
                return b.intValue();
            },
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final int x = a.intValue(), y = b.intValue();
                        return x >= y ? x : y;
                    }
                    return a.intValue();
                }
                return b.intValue();
            },
            (a, b) -> Integer.compare(a.intValue(), b.intValue()));

    static final NumberHelper LONG_NUMBER_HELPER = new NumberHelper(
            (a, b) -> Math.addExact(a.longValue(), b.longValue()),
            (a, b) -> Math.subtractExact(a.longValue(), b.longValue()),
            (a, b) -> Math.multiplyExact(a.longValue(), b.longValue()),
            (a, b) -> {
                if (a.longValue() == Long.MIN_VALUE && b.longValue() == -1) {
                    throw new ArithmeticException("long overflow");
                }
                return a.longValue() / b.longValue();
            },
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final long x = a.longValue(), y = b.longValue();
                        return x <= y ? x : y;
                    }
                    return a.longValue();
                }
                return b.longValue();
            },
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final long x = a.longValue(), y = b.longValue();
                        return x >= y ? x : y;
                    }
                    return a.longValue();
                }
                return b.longValue();
            },
            (a, b) -> Long.compare(a.longValue(), b.longValue()));

    static final NumberHelper BIG_INTEGER_NUMBER_HELPER = new NumberHelper(
            (a, b) -> bigIntegerValue(a).add(bigIntegerValue(b)),
            (a, b) -> bigIntegerValue(a).subtract(bigIntegerValue(b)),
            (a, b) -> bigIntegerValue(a).multiply(bigIntegerValue(b)),
            (a, b) -> bigIntegerValue(a).divide(bigIntegerValue(b)),
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final BigInteger x = bigIntegerValue(a), y = bigIntegerValue(b);
                        return x.compareTo(y) <= 0 ? x : y;
                    }
                    return bigIntegerValue(a);
                }
                return bigIntegerValue(b);
            },
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final BigInteger x = bigIntegerValue(a), y = bigIntegerValue(b);
                        return x.compareTo(y) >= 0 ? x : y;
                    }
                    return bigIntegerValue(a);
                }
                return bigIntegerValue(b);
            },
            (a, b) -> bigIntegerValue(a).compareTo(bigIntegerValue(b)));

    static final NumberHelper FLOAT_NUMBER_HELPER = new NumberHelper(
            (a, b) -> a.floatValue() + b.floatValue(),
            (a, b) -> a.floatValue() - b.floatValue(),
            (a, b) -> a.floatValue() * b.floatValue(),
            (a, b) -> a.floatValue() / b.floatValue(),
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final float x = a.floatValue(), y = b.floatValue();
                        return x <= y ? x : y;
                    }
                    return a.floatValue();
                }
                return b.floatValue();
            },
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final float x = a.floatValue(), y = b.floatValue();
                        return x >= y ? x : y;
                    }
                    return a.floatValue();
                }
                return b.floatValue();
            },
            (a, b) -> Float.compare(a.floatValue(), b.floatValue()));

    static final NumberHelper DOUBLE_NUMBER_HELPER = new NumberHelper(
            (a, b) -> a.doubleValue() + b.doubleValue(),
            (a, b) -> a.doubleValue() - b.doubleValue(),
            (a, b) -> a.doubleValue() * b.doubleValue(),
            (a, b) -> a.doubleValue() / b.doubleValue(),
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final double x = a.doubleValue(), y = b.doubleValue();
                        return x <= y ? x : y;
                    }
                    return a.doubleValue();
                }
                return b.doubleValue();
            },
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final double x = a.doubleValue(), y = b.doubleValue();
                        return x >= y ? x : y;
                    }
                    return a.doubleValue();
                }
                return b.doubleValue();
            },
            (a, b) -> Double.compare(a.doubleValue(), b.doubleValue()));

    static final NumberHelper BIG_DECIMAL_NUMBER_HELPER = new NumberHelper(
            (a, b) -> bigDecimalValue(a).add(bigDecimalValue(b)),
            (a, b) -> bigDecimalValue(a).subtract(bigDecimalValue(b)),
            (a, b) -> bigDecimalValue(a).multiply(bigDecimalValue(b)),
            (a, b) -> {
                final BigDecimal ba = bigDecimalValue(a);
                final BigDecimal bb = bigDecimalValue(b);
                try {
                    return ba.divide(bb);
                } catch (ArithmeticException ignored) {
                    // set a default precision
                    final int precision = Math.max(ba.precision(), bb.precision()) + 10;
                    BigDecimal result = ba.divide(bb, new MathContext(precision));
                    final int scale = Math.max(Math.max(ba.scale(), bb.scale()), 10);
                    if (result.scale() > scale) result = result.setScale(scale, BigDecimal.ROUND_HALF_UP);
                    return result;
                }
            },
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final BigDecimal x = bigDecimalValue(a), y = bigDecimalValue(b);
                        return x.compareTo(y) <= 0 ? x : y;
                    }
                    return bigDecimalValue(a);
                }
                return bigDecimalValue(b);
            },
            (a, b) -> {
                if (isNumber(a)) {
                    if (isNumber(b)) {
                        final BigDecimal x = bigDecimalValue(a), y = bigDecimalValue(b);
                        return x.compareTo(y) >= 0 ? x : y;
                    }
                    return bigDecimalValue(a);
                }
                return bigDecimalValue(b);
            },
            (a, b) -> {
                if( a instanceof Float && ((Float) a) == Float.POSITIVE_INFINITY)
                    return 1;
                else if( a instanceof Float && ((Float) a) == Float.NEGATIVE_INFINITY)
                    return -1;
                else if( b instanceof Float && ((Float) b) == Float.POSITIVE_INFINITY)
                    return -1;
                else if( b instanceof Float && ((Float) b) == Float.NEGATIVE_INFINITY)
                    return 1;
                else if( a instanceof Double && ((Double) a) == Double.POSITIVE_INFINITY)
                    return 1;
                else if( a instanceof Double && ((Double) a) == Double.NEGATIVE_INFINITY)
                    return -1;
                else if( b instanceof Double && ((Double) b) == Double.POSITIVE_INFINITY)
                    return -1;
                else if( b instanceof Double && ((Double) b) == Double.NEGATIVE_INFINITY)
                    return 1;
                return bigDecimalValue(a).compareTo(bigDecimalValue(b));
            });

    public final BiFunction<Number, Number, Number> add;
    public final BiFunction<Number, Number, Number> sub;
    public final BiFunction<Number, Number, Number> mul;
    public final BiFunction<Number, Number, Number> div;
    public final BiFunction<Number, Number, Number> min;
    public final BiFunction<Number, Number, Number> max;
    public final BiFunction<Number, Number, Integer> cmp;

    private NumberHelper(final BiFunction<Number, Number, Number> add,
                         final BiFunction<Number, Number, Number> sub,
                         final BiFunction<Number, Number, Number> mul,
                         final BiFunction<Number, Number, Number> div,
                         final BiFunction<Number, Number, Number> min,
                         final BiFunction<Number, Number, Number> max,
                         final BiFunction<Number, Number, Integer> cmp
    ) {
        this.add = add;
        this.sub = sub;
        this.mul = mul;
        this.div = div;
        this.min = min;
        this.max = max;
        this.cmp = cmp;
    }

    static NumberInfo getHighestCommonNumberInfo(final boolean forceFloatingPoint, final Number... numbers) {
        int bits = 8;
        boolean fp = forceFloatingPoint;
        for (final Number number : numbers) {
            if (!isNumber(number)) continue;
            final Class<? extends Number> clazz = number.getClass();
            if (clazz.equals(Byte.class)) continue;
            if (clazz.equals(Short.class)) {
                bits = Math.max(bits, 16);
            } else if (clazz.equals(Integer.class)) {
                bits = Math.max(bits, 32);
            } else if (clazz.equals(Long.class)) {
                bits = Math.max(bits, 64);
            } else if (clazz.equals(BigInteger.class)) {
                bits = 128;
            } else if (clazz.equals(Float.class)) {
                bits = Math.max(bits, 32);
                fp = true;
            } else if (clazz.equals(Double.class)) {
                bits = Math.max(bits, 64);
                fp = true;
            } else /*if (clazz.equals(BigDecimal.class))*/ {
                bits = 128;
                fp = true;
                break; // maxed out, no need to check remaining numbers
            }
        }
        return  new NumberInfo(bits, fp);
    }

    public static Class<? extends Number> getHighestCommonNumberClass(final Number... numbers) {
        return getHighestCommonNumberClass(false, numbers);
    }

    public static Class<? extends Number> getHighestCommonNumberClass(final boolean forceFloatingPoint, final Number... numbers) {
        NumberInfo numberInfo = getHighestCommonNumberInfo(forceFloatingPoint, numbers);
        return determineNumberClass(numberInfo.getBits(), numberInfo.getFp());
    }

    private static Number mathOperationWithPromote(final Function<NumberHelper, BiFunction<Number, Number, Number>> mathFunction, final boolean forceFloatingPoint, final Number a, final Number b) {
        if (null == a || null == b) return a;
        NumberInfo numberInfo = getHighestCommonNumberInfo(forceFloatingPoint, a, b);
        Number result = 0;
        while (true) {
            try {
                final Class<? extends Number> clazz = determineNumberClass(numberInfo.getBits(), numberInfo.getFp());
                final NumberHelper helper = getHelper(clazz);
                result = mathFunction.apply(helper).apply(a, b);
                if (result instanceof BigInteger || result instanceof BigDecimal)
                {
                    return result;
                }
                if (Double.isInfinite(result.doubleValue()))
                {
                    throw new ArithmeticException("Floating point overflow detected");
                }
                return result;
            } catch (ArithmeticException exception) {
                if (!numberInfo.getFp() && numberInfo.getBits() >= 64) {
                    throw exception;
                } else if (numberInfo.getFp() && numberInfo.getBits() >= 64) {
                    return result;
                }
                numberInfo.promoteBits();
            }
        }
    }

    /**
     * Adds two numbers returning the highest common number class between them.
     *
     * <p>
     * This method returns a result using the highest common number class between the two inputs.
     * If an overflow occurs (either integer or floating-point), the method promotes the precision
     * by increasing the bit width, until a suitable type is found.
     * If no suitable type exists (e.g., for very large integers beyond 64-bit),
     * an {@link ArithmeticException} is thrown. For floating-point numbers, if {@code double} overflows,
     * the result is {@code Double.POSITIVE_INFINITY} or {@code Double.NEGATIVE_INFINITY} instead of an exception.
     * </p>
     *
     * <pre>
     *     a = 1, b = 1 -> 2
     *     a = null, b = 1 -> null
     *     a = 1, b = null -> 1
     *     a = null, b = null -> null
     * </pre>
     *
     * @param a should be thought of as the seed to be modified by {@code b}
     * @param b the modifier to {code a}
     */
    public static Number add(final Number a, final Number b) {
        return mathOperationWithPromote(numberHelper -> numberHelper.add, false, a, b);
    }

    /**
     * Subtracts two numbers returning the highest common number class between them.
     *
     * <p>
     * This method returns a result using the highest common number class between the two inputs.
     * If an overflow occurs (either integer or floating-point), the method promotes the precision
     * by increasing the bit width, until a suitable type is found.
     * If no suitable type exists (e.g., for very large integers beyond 64-bit),
     * an {@link ArithmeticException} is thrown. For floating-point numbers, if {@code double} overflows,
     * the result is {@code Double.POSITIVE_INFINITY} or {@code Double.NEGATIVE_INFINITY} instead of an exception.
     * </p>
     *
     * <pre>
     *     a = 1, b = 1 -> 0
     *     a = null, b = 1 -> null
     *     a = 1, b = null -> 1
     *     a = null, b = null -> null
     * </pre>
     *
     * @param a should be thought of as the seed to be modified by {@code b}
     * @param b the modifier to {code a}
     */
    public static Number sub(final Number a, final Number b) {
        return mathOperationWithPromote(numberHelper -> numberHelper.sub, false, a, b);
    }

    /**
     * Multiplies two numbers returning the highest common number class between them.
     *
     * <p>
     * This method returns a result using the highest common number class between the two inputs.
     * If an overflow occurs (either integer or floating-point), the method promotes the precision
     * by increasing the bit width, until a suitable type is found.
     * If no suitable type exists (e.g., for very large integers beyond 64-bit),
     * an {@link ArithmeticException} is thrown. For floating-point numbers, if {@code double} overflows,
     * the result is {@code Double.POSITIVE_INFINITY} or {@code Double.NEGATIVE_INFINITY} instead of an exception.
     * </p>
     *
     * <pre>
     *     a = 1, b = 2 -> 2
     *     a = null, b = 1 -> null
     *     a = 1, b = null -> 1
     *     a = null, b = null -> null
     * </pre>
     *
     * @param a should be thought of as the seed to be modified by {@code b}
     * @param b the modifier to {code a}
     */
    public static Number mul(final Number a, final Number b) {
        return mathOperationWithPromote(numberHelper -> numberHelper.mul, false, a, b);
    }

    /**
     * Divides two numbers returning the highest common number class between them calling
     * {@link #div(Number, Number, boolean)} with a {@code false}.
     */
    public static Number div(final Number a, final Number b) {
        return mathOperationWithPromote(numberHelper -> numberHelper.div, false, a, b);
    }

    /**
     * Divides two numbers returning the highest common number class between them.
     *
     * <p>
     * This method returns a result using the highest common number class between the two inputs.
     * If an overflow occurs (either integer or floating-point), the method promotes the precision
     * by increasing the bit width, until a suitable type is found.
     * If no suitable type exists (e.g., for very large integers beyond 64-bit),
     * an {@link ArithmeticException} is thrown. For floating-point numbers, if {@code double} overflows,
     * the result is {@code Double.POSITIVE_INFINITY} or {@code Double.NEGATIVE_INFINITY} instead of an exception.
     * </p>
     *
     * <pre>
     *     a = 4, b = 2 -> 2
     *     a = null, b = 1 -> null
     *     a = 1, b = null -> 1
     *     a = null, b = null -> null
     * </pre>
     *
     * @param a should be thought of as the seed to be modified by {@code b}
     * @param b the modifier to {code a}
     * @param forceFloatingPoint when set to {@code true} ensures that the return value is the highest common floating number class
     */
    public static Number div(final Number a, final Number b, final boolean forceFloatingPoint) {
        return mathOperationWithPromote(numberHelper -> numberHelper.div, forceFloatingPoint, a, b);
    }

    /**
     * Gets the smaller number of the two provided returning the highest common number class between them.
     *
     * <pre>
     *     a = 4, b = 2 -> 2
     *     a = null, b = 1 -> 1
     *     a = 1, b = null -> 1
     *     a = null, b = null -> null
     *     a = NaN, b = 1 -> 1
     *     a = 1, b = NaN -> 1
     *     a = NaN, b = NaN -> NaN
     * </pre>
     */
    public static Number min(final Number a, final Number b) {
        // handle one or both null (propagate null if both)
        if (a == null || b == null)
            return a == null ? b : a;

        // handle one or both NaN (propagate NaN if both)
        if (eitherAreNaN(a, b))
            return isNaN(a) ? b : a;

        final Class<? extends Number> clazz = getHighestCommonNumberClass(a, b);
        return getHelper(clazz).min.apply(a, b);
    }

    /**
     * Gets the smaller number of the two provided returning the highest common number class between them.
     *
     * <pre>
     *     a = 4, b = 2 -> 2
     *     a = null, b = 1 -> 1
     *     a = 1, b = null -> 1
     *     a = null, b = null -> null
     *     a = NaN, b = 1 -> 1
     *     a = 1, b = NaN -> 1
     *     a = NaN, b = NaN -> NaN
     * </pre>
     */
    public static Comparable min(final Comparable a, final Comparable b) {
        // handle one or both null (propagate null if both)
        if (a == null || b == null)
            return a == null ? b : a;

        // handle one or both NaN (propagate NaN if both)
        if (eitherAreNaN(a, b))
            return isNaN(a) ? b : a;

        if (a instanceof Number && b instanceof Number) {
            final Number an = (Number) a, bn = (Number) b;
            final Class<? extends Number> clazz = getHighestCommonNumberClass(an, bn);
            return (Comparable) getHelper(clazz).min.apply(an, bn);
        } else {
            return a.compareTo(b) < 0 ? a : b;
        }
    }

    /**
     * Gets the larger number of the two provided returning the highest common number class between them.
     *
     * <pre>
     *     a = 4, b = 2 -> 4
     *     a = null, b = 1 -> 1
     *     a = 1, b = null -> 1
     *     a = null, b = null -> null
     *     a = NaN, b = 1 -> 1
     *     a = 1, b = NaN -> 1
     *     a = NaN, b = NaN -> NaN
     * </pre>
     */
    public static Number max(final Number a, final Number b) {
        // handle one or both null (propagate null if both)
        if (a == null || b == null)
            return a == null ? b : a;

        // handle one or both NaN (propagate NaN if both)
        if (eitherAreNaN(a, b))
            return isNaN(a) ? b : a;

        final Class<? extends Number> clazz = getHighestCommonNumberClass(a, b);
        return getHelper(clazz).max.apply(a, b);
    }

    /**
     * Gets the larger number of the two provided returning the highest common number class between them.
     *
     * <pre>
     *     a = 4, b = 2 -> 4
     *     a = null, b = 1 -> 1
     *     a = 1, b = null -> 1
     *     a = null, b = null -> null
     *     a = NaN, b = 1 -> 1
     *     a = 1, b = NaN -> 1
     *     a = NaN, b = NaN -> NaN
     * </pre>
     */
    public static Comparable max(final Comparable a, final Comparable b) {
        // handle one or both null (propagate null if both)
        if (a == null || b == null)
            return a == null ? b : a;

        // handle one or both NaN (propagate NaN if both)
        if (eitherAreNaN(a, b))
            return isNaN(a) ? b : a;

        if (a instanceof Number && b instanceof Number) {
            final Number an = (Number) a, bn = (Number) b;
            final Class<? extends Number> clazz = getHighestCommonNumberClass(an, bn);
            return (Comparable) getHelper(clazz).max.apply(an, bn);
        } else {
            return a.compareTo(b) > 0 ? a : b;
        }
    }

    /**
     * Compares two numbers. Follows orderability semantics for NaN, which places NaN after +Inf.
     *
     * <pre>
     *     a = 4, b = 2 -> 1
     *     a = 2, b = 4 -> -1
     *     a = null, b = 1 -> -1
     *     a = 1, b = null -> 1
     *     a = null, b = null -> 0
     *     a = NaN, b = NaN -> 0
     *     a = NaN, b = Inf -> 1
     * </pre>
     */
    public static Integer compare(final Number a, final Number b) {
        // handle one or both null
        if (a == null || b == null)
            return a == b ? 0 : (a == null ? -1 : 1);

        // handle one or both NaN
        if (eitherAreNaN(a, b))
            return (bothAreNaN(a, b)) ? 0 : isNaN(a) ? 1 : -1;

        final Class<? extends Number> clazz = getHighestCommonNumberClass(a, b);
        return getHelper(clazz).cmp.apply(a, b);
    }

    /**
     * Coerces the given number to the specified numeric type if it can fit into it.
     * Otherwise, retains the original type.
     *
     * @param a the number to be coerced
     * @param clazz the target numeric type class
     * @return the coerced number in the specified type or the original type if it cannot fit
     * @throws IllegalArgumentException if the specified numeric type is unsupported
     */
    public static Number coerceTo(final Number a, final Class<? extends Number> clazz) {
        try {
            return performConversion(a, clazz);
        } catch (ArithmeticException e) {
            // return as-is since it didn't fit the type we wanted to coerce to
            return a;
        }
    }

    /**
     * Casts the given number to the specified numeric type if it can fit into it.
     * Otherwise, throw.
     *
     * @param a         the number to be cast
     * @param typeToken the number token denoting the desired type to cast
     * @return the number cast to the specified type
     * @throws IllegalArgumentException if the specified numeric type is unsupported
     * @throws ArithmeticException      if the number overflows
     */
    public static Number castTo(final Number a, final GType typeToken) {
        Class<? extends Number> clazz = (Class<? extends Number>) typeToken.getType();
        return performConversion(a, clazz);
    }

    public static Number castTo(final Number a, final Class<? extends Number> clazz) {
        return performConversion(a, clazz);
    }

    /**
     * Core conversion logic.
     * Throws ArithmeticException when conversion would overflow.
     */
    private static Number performConversion(final Number a, final Class<? extends Number> clazz) {
        if (a.getClass().equals(clazz)) {
            return a;
        }
        if (clazz.equals(Integer.class)) {
            Long val = longValue(a, clazz);
            if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                return a.intValue();
            }
        } else if (clazz.equals(Long.class)) {
            return longValue(a, clazz);
        } else if (clazz.equals(Float.class)) {
            // BigDecimal or BigInteger to double can overflow into Infinity, we want to prevent this
            if (isInfinityOrNaN(a)) {
                return a.floatValue();
            }
            if (a.doubleValue() >= -Float.MAX_VALUE && a.doubleValue() <= Float.MAX_VALUE) {
                return a.floatValue();
            }
        } else if (clazz.equals(Double.class)) {
            // BigDecimal or BigInteger to double can overflow into Infinity, we want to prevent this
            if (isInfinityOrNaN(a)) {
                return a.doubleValue();
            }
            if (!Double.isInfinite(a.doubleValue())) {
                // float losses precision, use string intermediate
                return a.getClass().equals(Float.class) ? Double.parseDouble(a.toString()) : a.doubleValue();
            }
        } else if (clazz.equals(Byte.class)) {
            Long val = longValue(a, clazz);
            if (val >= Byte.MIN_VALUE && val <= Byte.MAX_VALUE) {
                return a.byteValue();
            }
        } else if (clazz.equals(Short.class)) {
            Long val = longValue(a, clazz);
            if (val >= Short.MIN_VALUE && val <= Short.MAX_VALUE) {
                return a.shortValue();
            }
        } else if (clazz.equals(BigInteger.class)) {
            return NumberHelper.bigIntegerValue(a);
        } else if (clazz.equals(BigDecimal.class)) {
            return NumberHelper.bigDecimalValue(a);
        } else {
            throw new IllegalArgumentException("Unsupported numeric type: " + clazz);
        }

        throw new ArithmeticException(String.format("Can't convert number of type %s to %s due to overflow.",
                a.getClass().getSimpleName(), clazz.getSimpleName()));
    }

    private static Long longValue(final Number num, final Class<? extends Number> targetClass) {
        // Explicitly throw when converting floating point infinity and NaN to whole numbers
        if (Double.isNaN(num.doubleValue())) {
            throw new ArithmeticException(String.format("Can't convert NaN to %s.", targetClass.getSimpleName()));
        }
        if (Double.isInfinite(num.doubleValue())) {
            throw new ArithmeticException(String.format("Can't convert floating point infinity to %s.", targetClass.getSimpleName()));
        }
        String msgOverflow = String.format("Can't convert number of type %s to %s due to overflow.",
                num.getClass().getSimpleName(), targetClass.getSimpleName());

        if (num.getClass().equals(Double.class) || num.getClass().equals(Float.class)) {
            double value = num.doubleValue();
            if (value > Long.MAX_VALUE) {
                throw new ArithmeticException(msgOverflow);
            }
            if (value < Long.MIN_VALUE) {
                throw new ArithmeticException(String.format("Can't convert number of type %s to %s due to underflow.",
                        num.getClass().getSimpleName(), targetClass.getSimpleName()));
            }
        }

        try {
            if (num.getClass().equals(BigDecimal.class)) {
                // need to truncate the decimal places or else longValueExact() will throw
                BigDecimal truncated = ((BigDecimal) num).setScale(0, RoundingMode.DOWN);
                return truncated.longValueExact();
            }

            return num.getClass().equals(BigInteger.class) ? ((BigInteger) num).longValueExact() : num.longValue();
        } catch (ArithmeticException ae) {
            throw new ArithmeticException(msgOverflow);
        }

    }

    private static boolean isInfinityOrNaN(Number num) {
        return (!num.getClass().equals(BigDecimal.class) && !num.getClass().equals(BigInteger.class) &&
                (Double.isInfinite(num.doubleValue()) || Double.isNaN(num.doubleValue())));
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
        if (number instanceof BigDecimal) return ((BigDecimal) number).toBigInteger();
        if (number instanceof Double) return BigDecimal.valueOf(number.doubleValue()).toBigInteger();
        return BigInteger.valueOf(number.longValue());
    }

    private static BigDecimal bigDecimalValue(final Number number) {
        if (number == null) return null;
        if (number instanceof BigDecimal) return (BigDecimal) number;
        if (number instanceof BigInteger) return new BigDecimal((BigInteger) number);
        if (number instanceof Float) return new BigDecimal(number.toString()); // float losses precision, use string intermediate
        return (number instanceof Double)
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

    private static boolean isNumber(final Number number) {
        return number != null && !isNaN(number);
    }

    public static boolean isNaN(final Object object) {
        return (object instanceof Float && Float.isNaN((float) object)) ||
                (object instanceof Double && Double.isNaN((double) object));
    }

    public static boolean eitherAreNaN(final Object first, final Object second) {
        return isNaN(first) || isNaN(second);
    }

    public static boolean bothAreNaN(final Object first, final Object second) {
        return isNaN(first) && isNaN(second);
    }

    public static boolean isPositiveInfinity(final Object value) {
        return (value instanceof Float && Float.POSITIVE_INFINITY == ((float) value)) ||
                (value instanceof Double && Double.POSITIVE_INFINITY == ((double) value));
    }

    public static boolean isNegativeInfinity(final Object value) {
        return (value instanceof Float && Float.NEGATIVE_INFINITY == ((float) value)) ||
                (value instanceof Double && Double.NEGATIVE_INFINITY == ((double) value));
    }
}
