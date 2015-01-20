package com.tinkerpop.gremlin.console.groovy.plugin;

import com.tinkerpop.gremlin.groovy.plugin.SugarGremlinPlugin;
import com.tinkerpop.gremlin.groovy.util.MetaRegistryUtil;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * This test lives here (even though the SugarPlugin is over in gremlin-groovy, because it really tests the
 * plugin with respect to the Console.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SugarGremlinPluginTest {
    @Test
    public void shouldPluginSugar() throws Exception {
        MetaRegistryUtil.clearRegistry(new HashSet<>(Arrays.asList(TinkerGraph.class)));

        final SugarGremlinPlugin plugin = new SugarGremlinPlugin();

        final Groovysh groovysh = new Groovysh();

        final Map<String, Object> env = new HashMap<>();
        env.put("ConsolePluginAcceptor.io", new IO());
        env.put("ConsolePluginAcceptor.shell", groovysh);

        final SpyPluginAcceptor spy = new SpyPluginAcceptor(groovysh::execute, () -> env);
        plugin.pluginTo(spy);

        groovysh.getInterp().getContext().setProperty("g", TinkerFactory.createClassic());
        assertEquals(6l, ((GraphTraversal) groovysh.execute("g.V()")).count().next());
        assertEquals(6l, ((GraphTraversal) groovysh.execute("g.V")).count().next());
    }
}
