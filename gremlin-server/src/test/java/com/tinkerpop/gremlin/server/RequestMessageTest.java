package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
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
        final RequestMessage msg = RequestMessage.create("op").overrideRequestId(request).build();
        assertEquals(request, msg.getRequestId());
    }

    @Test
    public void shouldSetProcessor() {
        final RequestMessage msg = RequestMessage.create("op").setProcessor("ppp").build();
        assertEquals("ppp", msg.getProcessor());
    }

    @Test
    public void shouldSetOpWithDefaults() {
        final RequestMessage msg = RequestMessage.create("op").build();
        assertEquals(StandardOpProcessor.OP_PROCESSOR_NAME, msg.getProcessor());
        assertNotNull(msg.getRequestId());
        assertEquals("op", msg.getOp());
        assertNotNull(msg.getArgs());
    }

    @Test
    public void shouldReturnEmptyOptionalArg() {
        final RequestMessage msg = RequestMessage.create("op").build();
        assertFalse(msg.optionalArgs("test").isPresent());
    }

    @Test
    public void shouldReturnArgAsOptional() {
        final RequestMessage msg = RequestMessage.create("op").add("test", "testing").build();
        assertEquals("testing", msg.optionalArgs("test").get());
    }
}
