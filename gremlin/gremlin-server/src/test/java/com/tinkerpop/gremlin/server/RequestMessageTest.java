package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.server.util.ser.JsonMessageSerializerV1d0;
import org.junit.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

    @Test
    public void shouldParseMessageNicelyWithNoArgs() {
        final UUID session = UUID.fromString("7F5C73E3-CB3C-45A7-BF76-F6A68F944A8A");
        final UUID request = UUID.fromString("011CFEE9-F640-4844-AC93-034448AC0E80");
        final Optional<RequestMessage> msg = new JsonMessageSerializerV1d0().deserializeRequest(String.format("{\"sessionId\":\"%s\",\"requestId\":\"%s\",\"op\":\"eval\"}", session, request));
        assertTrue(msg.isPresent());

        final RequestMessage m = msg.get();
        assertEquals(session, m.sessionId);
        assertEquals(request, m.requestId);
        assertEquals("eval", m.op);
        assertNotNull(m.args);
        assertEquals(0, m.args.size());
    }

    @Test
    public void shouldParseMessageNicelyWithArgs() {
        final UUID session = UUID.fromString("7F5C73E3-CB3C-45A7-BF76-F6A68F944A8A");
        final UUID request = UUID.fromString("011CFEE9-F640-4844-AC93-034448AC0E80");
        final Optional<RequestMessage> msg = new JsonMessageSerializerV1d0().deserializeRequest(String.format("{\"sessionId\":\"%s\",\"requestId\":\"%s\",\"op\":\"eval\",\"args\":{\"x\":\"y\"}}", session, request));
        assertTrue(msg.isPresent());

        final RequestMessage m = msg.get();
        assertEquals(session, m.sessionId);
        assertEquals(request, m.requestId);
        assertEquals("eval", m.op);
        assertNotNull(m.args);
        assertEquals("y", m.args.get("x"));
    }

    @Test
    public void shouldNotParseMessage() {
        final Optional<RequestMessage> msg = new JsonMessageSerializerV1d0().deserializeRequest("{\"sessionId\":\"%s\",\"requestId\":\"%s\",\"op\":\"eval\",\"args\":{\"x\":\"y\"}}");
        assertFalse(msg.isPresent());
    }
}
