package com.tinkerpop.gremlin.driver.message;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Helps identify the type of item in the {@link com.tinkerpop.gremlin.driver.message.ResponseMessage#getResult()}.
 * In this way, results from iterated results can be batched for return to the client.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum ResultType {
    OBJECT(0),
    COLLECTION(1),
    EMPTY(2);

    private final int value;
    private static Map<Integer, ResultType> codeValueMap = new HashMap<>();

    static {
        Stream.of(ResultType.values()).forEach(code -> codeValueMap.put(code.getValue(), code));
    }

    public static ResultType getFromValue(final int codeValue) {
        return codeValueMap.get(codeValue);
    }

    private ResultType(final int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
