package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.server.message.RequestMessage;
import com.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RequestMessageTest {

    @Test
    public void shouldOverrideRequest() {
        final UUID request = UUID.randomUUID();
        final RequestMessage msg = new RequestMessage.Builder("op").overrideRequestId(request).build();
        assertEquals(request, msg.requestId);
    }

    @Test
    public void shouldSetProcessor() {
        final RequestMessage msg = new RequestMessage.Builder("op").setProcessor("ppp").build();
        assertEquals("ppp", msg.processor);
    }

    @Test
    public void shouldSetOpWithDefaults() {
        final RequestMessage msg = new RequestMessage.Builder("op").build();
        assertEquals(StandardOpProcessor.OP_PROCESSOR_NAME, msg.processor);
        assertNotNull(msg.requestId);
        assertEquals("op", msg.op);
        assertNotNull(msg.args);
    }

    @Test
    public void shouldReturnEmptyOptionalArg() {
        final RequestMessage msg = new RequestMessage.Builder("op").build();
        assertFalse(msg.optionalArgs("test").isPresent());
    }

    @Test
    public void shouldReturnArgAsOptional() {
        final RequestMessage msg = new RequestMessage.Builder("op").build();
        msg.args.put("test", "testing");
        assertEquals("testing", msg.optionalArgs("test").get());
    }
}
