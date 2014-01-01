package com.tinkerpop.gremlin.server;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerIntegrateTest extends AbstractGremlinServerIntegrationTest {

    @Rule
    public TestName name = new TestName();

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();
        if (nameOfTest.equals("shouldReceiveFailureTimeOutOnScriptEval"))
            settings.scriptEvaluationTimeout = 200;
        else if (nameOfTest.equals("shouldReceiveFailureTimeOutOnTotalSerialization"))
            settings.serializedResponseTimeout = 1;
        else if (nameOfTest.equals("shouldReceiveFailureForTimeoutOfIndividualSerialization"))
            settings.serializeResultTimeout = 1;

        return settings;
    }

    @Test
    public void shouldReceiveFailureTimeOutOnScriptEval() throws Exception {
        final String url = getWebSocketBaseUri();
        final WebSocketClient client = new WebSocketClient(url);
        client.open();

        // todo: better error handling should be in the "real" client.  adjust the assertion when that happens.
        final String result = client.<String>eval("Thread.sleep(300);'some-stuff-that-should not return'").findFirst().orElse("nothing");
        assertTrue(result.startsWith("Script evaluation exceeded the configured threshold of 200 ms for request"));

        client.close();
    }

    @Test
    public void shouldReceiveFailureTimeOutOnTotalSerialization() throws Exception {
        final String url = getWebSocketBaseUri();
        final WebSocketClient client = new WebSocketClient(url);
        client.open();

        // todo: better error handling should be in the "real" client.  adjust the assertion when that happens.
        final List<String> results = client.<String>eval("(0..<100000)").collect(Collectors.toList());

        // the last item in the list is the error
        final String result = results.get(results.size() - 1);
        assertTrue(result.endsWith("Serialization of the entire response exceeded the serializeResponseTimeout setting"));

        client.close();
    }

    @Test
    public void shouldReceiveFailureForTimeoutOfIndividualSerialization() throws Exception {
        final String url = getWebSocketBaseUri();
        final WebSocketClient client = new WebSocketClient(url);
        client.open();

        // todo: better error handling should be in the "real" client.  adjust the assertion when that happens.
        final String result = client.<String>eval("[(0..<100000),'x']").findFirst().orElse("nothing");
        assertTrue(result.endsWith("Serialization of an individual result exceeded the serializeResultTimeout setting"));

        client.close();
    }
}
