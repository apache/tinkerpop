package com.tinkerpop.gremlin.process.util;

import java.util.Random;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RandomString {

    private final Random random;
    private static final char[] SYMBOLS;
    private static final char[] ALPHA_SYMBOLS;

    static {
        final StringBuilder alphaSymbols = new StringBuilder();
        final StringBuilder symbols = new StringBuilder();
        for (char ch = '0'; ch <= '9'; ++ch)
            symbols.append(ch);
        for (char ch = 'a'; ch <= 'z'; ++ch) {
            alphaSymbols.append(ch);
            symbols.append(ch);
        }
        SYMBOLS = symbols.toString().toCharArray();
        ALPHA_SYMBOLS = alphaSymbols.toString().toCharArray();
    }

    public RandomString(final int seed) {
        this.random = new Random(seed);
    }

    public String nextString(int length) {
        if (length < 1) throw new IllegalArgumentException("The string length must be 1 or greater");
        final StringBuilder builder = new StringBuilder();
        builder.append(ALPHA_SYMBOLS[random.nextInt(ALPHA_SYMBOLS.length)]); // always prefix with character (for directory purposes)
        for (int i = 0; i < length - 1; i++) {
            builder.append(SYMBOLS[random.nextInt(SYMBOLS.length)]);
        }
        return builder.toString();
    }
}
