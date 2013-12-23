package com.tinkerpop.gremlin.server;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerIntegrateTest extends AbstractGremlinServerIntegrationTest {
    @Test
    public void shouldReceiveFailureTimeOutOnScriptEval() throws Exception {
        final String url = getWebSocketBaseUri();
        final WebSocketClient client = new WebSocketClient(url);
        client.open();

        // todo: better error handling should be in the "real" client.  adjust the assertion when that happens.
        final String result = client.<String>eval("Thread.sleep(11000);'some-stuff-that-should not return'").findFirst().orElse("nothing");
        System.out.println(result);
        assertTrue(result.startsWith("Script evaluation exceeded the configured threshold of 10000 ms for request"));

        client.close();
    }
}
