package com.tinkerpop.gremlin.driver.ser;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum Serializers {
    JSON("application/json"),
    JSON_V1D0("application/vnd.gremlin-v1.0+json"),
    KRYO_V1D0("application/vnd.gremlin-v1.0+kryo");

    private String value;

    private Serializers(final String mimeType) {
        this.value = mimeType;
    }

    public String getValue() {
        return value;
    }
}
