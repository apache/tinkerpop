package com.tinkerpop.gremlin.driver.message;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum ResponseContents {
    OBJECT(0),
    COLLECTION(1);

    private final int value;
    private static Map<Integer, ResponseContents> codeValueMap = new HashMap<>();

    static {
        Stream.of(ResponseContents.values()).forEach(code -> codeValueMap.put(code.getValue(), code));
    }

    public static ResponseContents getFromValue(final int codeValue) {
        return codeValueMap.get(codeValue);
    }

    private ResponseContents(final int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
