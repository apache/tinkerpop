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
import java.math.MathContext;
import java.util.function.BiFunction;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class NumberHelper {

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
            (a, b) -> (byte) (a.byteValue() / b.byteValue()),
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
            (a, b) -> (short) (a.shortValue() / b.shortValue()),
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
            (a, b) -> a.intValue() / b.intValue(),
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
            (a, b) -> a.longValue() / b.longValue(),
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

    public static Class<? extends Number> getHighestCommonNumberClass(final Number... numbers) {
        return getHighestCommonNumberClass(false, numbers);
    }

    public static Class<? extends Number> getHighestCommonNumberClass(final boolean forceFloatingPoint, final Number... numbers) {
        int bits = 8;
        boolean fp = forceFloatingPoint;
        for (final Number number : numbers) {
            if (!isNumber(number)) continue;
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

    /**
     * Adds two numbers returning the highest common number class between them.
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
        if (null == a || null == b) return a;
        final Class<? extends Number> clazz = getHighestCommonNumberClass(a, b);
        return getHelper(clazz).add.apply(a, b);
    }


    /**
     * Subtracts two numbers returning the highest common number class between them.
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
        if (null == a || null == b) return a;
        final Class<? extends Number> clazz = getHighestCommonNumberClass(a, b);
        return getHelper(clazz).sub.apply(a, b);
    }

    /**
     * Multiplies two numbers returning the highest common number class between them.
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
        if (null == a || null == b) return a;
        final Class<? extends Number> clazz = getHighestCommonNumberClass(a, b);
        return getHelper(clazz).mul.apply(a, b);
    }

    /**
     * Divides two numbers returning the highest common number class between them calling
     * {@link #div(Number, Number, boolean)} with a {@code false}.
     */
    public static Number div(final Number a, final Number b) {
        if (null == a || null == b) return a;
        return div(a, b, false);
    }

    /**
     * Divides two numbers returning the highest common number class between them.
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
        if (null == a || null == b) return null;
        final Class<? extends Number> clazz = getHighestCommonNumberClass(forceFloatingPoint, a, b);
        return getHelper(clazz).div.apply(a, b);
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
        if (a.getClass().equals(clazz)) {
            return a;
        } else if (clazz.equals(Integer.class)) {
            if (a.longValue() >= Integer.MIN_VALUE && a.longValue() <= Integer.MAX_VALUE) {
                return a.intValue();
            }
        } else if (clazz.equals(Long.class)) {
            return a.longValue();
        } else if (clazz.equals(Float.class)) {
            if (a.doubleValue() >= -Float.MAX_VALUE && a.doubleValue() <= Float.MAX_VALUE) {
                return a.floatValue();
            }
        } else if (clazz.equals(Double.class)) {
            return a.doubleValue();
        } else if (clazz.equals(Byte.class)) {
            if (a.longValue() >= Byte.MIN_VALUE && a.longValue() <= Byte.MAX_VALUE) {
                return a.byteValue();
            }
        } else if (clazz.equals(Short.class)) {
            if (a.longValue() >= Short.MIN_VALUE && a.longValue() <= Short.MAX_VALUE) {
                return a.shortValue();
            }
        } else if (clazz.equals(BigInteger.class)) {
            return NumberHelper.bigIntegerValue(a);
        } else if (clazz.equals(BigDecimal.class)) {
            return NumberHelper.bigDecimalValue(a);
        } else {
            throw new IllegalArgumentException("Unsupported numeric type: " + clazz);
        }

        // return as-is since it didn't fit the type we wanted to coerce to
        return a;
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
