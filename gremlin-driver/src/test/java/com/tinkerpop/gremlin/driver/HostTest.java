package com.tinkerpop.gremlin.driver;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HostTest {

    @Test
    public void shouldConstructHost() {
        final InetSocketAddress addy = new InetSocketAddress("localhost", 8182);
        final Host host = new Host(addy);
        final URI webSocketUri = host.getWebSocketUri();
        assertEquals("ws://localhost:8182/gremlin", webSocketUri.toString());
    }

}
