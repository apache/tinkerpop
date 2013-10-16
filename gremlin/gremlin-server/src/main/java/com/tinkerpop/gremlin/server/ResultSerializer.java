package com.tinkerpop.gremlin.server;

/**
 * Serializes a single result from the ScripEngine.  Typically this will be an item from an iterator.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ResultSerializer {
    public static final ToStringResultSerializer TO_STRING_RESULT_SERIALIZER = new ToStringResultSerializer();

    public String serialize(final Object o, final Context context);

    /**
     * Use toString() to serialize the result.
     */
    public static class ToStringResultSerializer implements ResultSerializer {
        @Override
        public String serialize(Object o, Context context) {
            return o == null ? "null" : o.toString();
        }
    }

}
