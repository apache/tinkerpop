package com.tinkerpop.gremlin.structure.util.batch.cache;

/**
 * @author Matthias Broecheler (http://www.matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface StringCompression {
    public static final StringCompression NO_COMPRESSION = input -> input;

    public String compress(final String input);
}
