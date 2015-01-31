package com.tinkerpop.gremlin.console.groovy.plugin;

import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverGremlinPluginTest {
    @Test
    public void shouldPluginToAndDoImports() throws Exception {
        final DriverGremlinPlugin plugin = new DriverGremlinPlugin();
        final SpyPluginAcceptor spy = new SpyPluginAcceptor();
        plugin.pluginTo(spy);

        assertEquals(4, spy.getImports().size());
        spy.getImports().forEach(importString -> assertThat(importString, containsString("com.tinkerpop.gremlin.driver")));
    }

    @Test
    public void shouldConstructEmptyRemoteAcceptorWhenNotInConsoleEnvironment() throws Exception {
        final DriverGremlinPlugin plugin = new DriverGremlinPlugin();
        final SpyPluginAcceptor spy = new SpyPluginAcceptor();
        plugin.pluginTo(spy);

        assertThat(plugin.remoteAcceptor().isPresent(), is(false));
    }

    @Test
    public void shouldConstructRemoteAcceptorWhenInConsoleEnvironment() throws Exception {
        final DriverGremlinPlugin plugin = new DriverGremlinPlugin();
        final Map<String, Object> env = new HashMap<>();
        env.put("ConsolePluginAcceptor.io", new IO());
        env.put("ConsolePluginAcceptor.shell", new Groovysh());
        final SpyPluginAcceptor spy = new SpyPluginAcceptor(() -> env);
        plugin.pluginTo(spy);

        assertThat(plugin.remoteAcceptor().isPresent(), is(true));
    }
}
