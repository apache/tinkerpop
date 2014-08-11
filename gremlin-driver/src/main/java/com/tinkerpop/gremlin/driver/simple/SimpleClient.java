package com.tinkerpop.gremlin.driver.simple;

import com.tinkerpop.gremlin.driver.Tokens;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;

import java.io.Closeable;
import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface SimpleClient extends Closeable {

    public default void submit(final String gremlin, final Consumer<ResponseMessage> callback) throws Exception {
        submit(RequestMessage.build(Tokens.OPS_EVAL).addArg(Tokens.ARGS_GREMLIN, gremlin).create(), callback);
    }

    public void submit(final RequestMessage requestMessage, final Consumer<ResponseMessage> callback) throws Exception;
}
