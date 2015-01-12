package com.tinkerpop.gremlin.console.groovy.plugin;

import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class UtilitiesGremlinPluginTest {
    @Test
    public void shouldPluginToAndDoImports() throws Exception {
        final UtilitiesGremlinPlugin plugin = new UtilitiesGremlinPlugin();
        final SpyPluginAcceptor spy = new SpyPluginAcceptor();
        plugin.pluginTo(spy);

        assertEquals(4, spy.getImports().size());
    }

    @Test
    public void shouldFailWithoutUtilitiesPlugin() throws Exception {
        final Groovysh groovysh = new Groovysh();
        try {
            groovysh.execute("describeGraph(g.class)");
            fail("Utilities were not loaded - this should fail.");
        } catch (Exception ignored) {
        }
    }

    @Test
    public void shouldPluginUtilities() throws Exception {
        final UtilitiesGremlinPlugin plugin = new UtilitiesGremlinPlugin();

        final Groovysh groovysh = new Groovysh();
        groovysh.getInterp().getContext().setProperty("g", TinkerFactory.createClassic());

        final Map<String, Object> env = new HashMap<>();
        env.put("ConsolePluginAcceptor.io", new IO());
        env.put("ConsolePluginAcceptor.shell", groovysh);

        final SpyPluginAcceptor spy = new SpyPluginAcceptor(groovysh::execute, () -> env);
        plugin.pluginTo(spy);

        assertThat(groovysh.execute("describeGraph(com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph)").toString(), containsString("IMPLEMENTATION - com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph"));
        assertThat(groovysh.execute("clock {g.V().count().next()}"), is(instanceOf(Number.class)));
    }
}
