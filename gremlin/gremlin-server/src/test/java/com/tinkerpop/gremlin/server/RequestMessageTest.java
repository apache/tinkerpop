package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RequestMessageTest {
    @Test
    public void shouldReturnEmptyOptionalSessionId() {
        final RequestMessage msg = new RequestMessage.Builder("op").noSession().build();
        assertFalse(msg.optionalSessionId().isPresent());
    }

    @Test
    public void shouldReturnSessionIdAsOptional() {
        final RequestMessage msg = new RequestMessage.Builder("op").newSession().build();
        assertTrue(msg.optionalSessionId().isPresent());
        assertEquals(msg.sessionId, msg.optionalSessionId().get());
    }

    @Test
    public void shouldOverrideSession() {
        final UUID session = UUID.randomUUID();
        final RequestMessage msg = new RequestMessage.Builder("op").existingSession(session).build();
        assertEquals(session, msg.sessionId);
    }

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
        assertNull(msg.sessionId);
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
