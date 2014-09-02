package com.tinkerpop.gremlin.driver.ser;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SerTokens {
    public static final String TOKEN_ATTRIBUTES = "attributes";
    public static final String TOKEN_RESULT = "result";
    public static final String TOKEN_STATUS = "status";
    public static final String TOKEN_DATA = "data";
    public static final String TOKEN_META = "meta";
    public static final String TOKEN_CODE = "code";
    public static final String TOKEN_REQUEST = "requestId";
    public static final String TOKEN_MESSAGE = "message";
    public static final String TOKEN_PROCESSOR = "processor";
    public static final String TOKEN_OP = "op";
    public static final String TOKEN_ARGS = "args";

    public static final String MIME_JSON = "application/json";
    public static final String MIME_JSON_V1D0 = "application/vnd.gremlin-v1.0+json";
    public static final String MIME_KRYO_V1D0 = "application/vnd.gremlin-v1.0+kryo";
}
