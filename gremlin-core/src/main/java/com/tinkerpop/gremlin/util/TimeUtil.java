package com.tinkerpop.gremlin.util;

import java.util.concurrent.TimeUnit;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TimeUtil {
    public static long secondsSince(final long startNanos) {
        return timeSince(startNanos, TimeUnit.SECONDS);
    }

    public static long millisSince(final long startNanos) {
        return timeSince(startNanos, TimeUnit.MILLISECONDS);
    }

    public static long minutesSince(final long startNanos) {
        return timeSince(startNanos, TimeUnit.MINUTES);
    }

    public static long timeSince(final long startNanos, final TimeUnit destUnit) {
        return destUnit.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }
}
