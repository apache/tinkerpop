package com.tinkerpop.gremlin.driver.message;

import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResponseResult {
    private final Object data;
    private final Map<String, Object> meta;

    public ResponseResult(final Object data, final Map<String, Object> meta) {
        this.data = data;
        this.meta = meta;
    }

    public Object getData() {
        return data;
    }

    public Map<String, Object> getMeta() {
        return meta;
    }

    @Override
    public String toString() {
        return "ResponseResult{" +
                "data=" + data +
                ", meta=" + meta +
                '}';
    }
}
