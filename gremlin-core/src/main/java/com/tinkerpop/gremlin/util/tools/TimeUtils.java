package com.tinkerpop.gremlin.util.tools;

import java.util.stream.IntStream;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class TimeUtils {

    public static double clock(final Runnable runnable) {
        return clock(100, runnable);
    }

    public static double clock(final int loops, final Runnable runnable) {
        runnable.run(); // warm-up
        return IntStream.range(0, loops).mapToDouble(i -> {
            long t = System.nanoTime();
            runnable.run();
            return (System.nanoTime() - t) * 0.000001;
        }).sum() / loops;
    }
}
