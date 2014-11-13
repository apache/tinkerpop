package com.tinkerpop.gremlin.console.plugin;

import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SugarGremlinPluginTest {
    @Test
    public void shouldPluginSugar() throws Exception {
        final SugarGremlinPlugin plugin = new SugarGremlinPlugin();

        final Groovysh groovysh = new Groovysh();

        final Map<String,Object> env = new HashMap<>();
        env.put("ConsolePluginAcceptor.io", new IO());
        env.put("ConsolePluginAcceptor.shell", groovysh);

        final SpyPluginAcceptor spy = new SpyPluginAcceptor(groovysh::execute, () -> env);
        plugin.pluginTo(spy);

        groovysh.getInterp().getContext().setProperty("g", TinkerFactory.createClassic());
        assertEquals(6l, groovysh.execute("g.V().count().next()"));
        assertEquals(6l, groovysh.execute("g.V.count().next()"));
    }
}
