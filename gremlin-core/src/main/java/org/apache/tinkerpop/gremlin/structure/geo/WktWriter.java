/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.geo;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * Package-private WKT writer.
 */
final class WktWriter {

    // 17 significant digits always read back to the same double (IEEE 754)
    private static final int MAX_SIGNIFICANT_DIGITS = 17;

    private WktWriter() {
    }

    /**
     * Writes a geometry to WKT format.
     */
    static String write(final Geometry g) {
        final StringBuilder sb = new StringBuilder();
        writeTagged(sb, g);
        return sb.toString();
    }

    /**
     * Appends a geometry with its type name, its Z tag and either the EMPTY keyword or a
     * parenthesized body. The choice is structural, so WKT and WKB describe the same parts.
     */
    static void writeTagged(final StringBuilder sb, final Geometry g) {
        sb.append(g.wktTypeName());
        if (g.hasZ()) {
            sb.append(" Z");
        }
        if (g.hasNoParts()) {
            sb.append(" EMPTY");
        } else {
            if (g.hasZ()) {
                sb.append(' ');
            }
            sb.append('(');
            g.writeWktBody(sb);
            sb.append(')');
        }
    }

    /**
     * Formats a double value using the shortest decimal representation that
     * parses back to the same double value. No exponent notation, no trailing .0.
     * Handles -0.0 as "-0".
     */
    static String formatDouble(final double value) {
        if (Double.isNaN(value)) {
            throw new IllegalStateException("Cannot format NaN in WKT");
        }
        if (Double.isInfinite(value)) {
            throw new IllegalStateException("Cannot format Infinity in WKT");
        }

        if (value == 0.0) {
            // negative zero keeps its sign
            if (Double.doubleToRawLongBits(value) == Long.MIN_VALUE) {
                return "-0";
            }
            return "0";
        }

        // The exact binary expansion of the value, so the search measures candidates against
        // the value itself rather than against a decimal approximation of it.
        final BigDecimal exact = new BigDecimal(value);
        return shortestRoundTrip(exact, value).stripTrailingZeros().toPlainString();
    }

    /**
     * Returns the fewest-digit decimal that reads back as the given double.
     * <p>
     * At each precision the only two candidates that can read back are the two decimals of
     * that length nearest the value, which are the exact expansion rounded down and rounded
     * up. The first precision at which one of them reads back wins. Where both read back,
     * the candidate nearer the exact value wins, and an exact tie goes to the even last digit.
     */
    private static BigDecimal shortestRoundTrip(final BigDecimal exact, final double value) {
        for (int digits = 1; digits <= MAX_SIGNIFICANT_DIGITS; digits++) {
            final BigDecimal down = exact.round(new MathContext(digits, RoundingMode.FLOOR));
            final BigDecimal up = exact.round(new MathContext(digits, RoundingMode.CEILING));
            final boolean downReadsBack = down.doubleValue() == value;
            final boolean upReadsBack = up.doubleValue() == value;

            if (downReadsBack && upReadsBack) {
                return nearer(exact, down, up);
            }
            if (downReadsBack) {
                return down;
            }
            if (upReadsBack) {
                return up;
            }
        }
        throw new IllegalStateException("No decimal of " + MAX_SIGNIFICANT_DIGITS
                + " or fewer digits reads back as " + value);
    }

    /**
     * Returns whichever of the two candidates lies nearer the exact value, preferring the
     * one whose last digit is even when they are equally near.
     */
    private static BigDecimal nearer(final BigDecimal exact, final BigDecimal down, final BigDecimal up) {
        final int comparison = exact.subtract(down).compareTo(up.subtract(exact));
        if (comparison < 0) {
            return down;
        }
        if (comparison > 0) {
            return up;
        }
        // equally near, which holds when the candidates are the same value, or when they
        // differ by one unit in the last place and so have different parity
        return down.unscaledValue().testBit(0) ? up : down;
    }
}
