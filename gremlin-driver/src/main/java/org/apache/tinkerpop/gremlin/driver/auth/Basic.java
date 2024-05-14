package org.apache.tinkerpop.gremlin.driver.auth;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;

import java.util.Base64;

public class Basic implements Auth {

    private final String username;
    private final String password;

    public Basic(final String username, final String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public FullHttpRequest apply(final FullHttpRequest fullHttpRequest) {
        final String valueToEncode = username + ":" + password;
        fullHttpRequest.headers().add(HttpHeaderNames.AUTHORIZATION,
                "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes()));
        return fullHttpRequest;
    }
}
