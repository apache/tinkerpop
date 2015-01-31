package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RequestMessageTest {

    @Test
    public void shouldOverrideRequest() {
        final UUID request = UUID.randomUUID();
        final RequestMessage msg = RequestMessage.build("op").overrideRequestId(request).create();
        assertEquals(request, msg.getRequestId());
    }

    @Test
    public void shouldSetProcessor() {
        final RequestMessage msg = RequestMessage.build("op").processor("ppp").create();
        assertEquals("ppp", msg.getProcessor());
    }

    @Test
    public void shouldSetOpWithDefaults() {
        final RequestMessage msg = RequestMessage.build("op").create();
        Assert.assertEquals("", msg.getProcessor());    // standard op processor
        assertNotNull(msg.getRequestId());
        assertEquals("op", msg.getOp());
        assertNotNull(msg.getArgs());
    }

    @Test
    public void shouldReturnEmptyOptionalArg() {
        final RequestMessage msg = RequestMessage.build("op").create();
        assertFalse(msg.optionalArgs("test").isPresent());
    }

    @Test
    public void shouldReturnArgAsOptional() {
        final RequestMessage msg = RequestMessage.build("op").add("test", "testing").create();
        assertEquals("testing", msg.optionalArgs("test").get());
    }
}
