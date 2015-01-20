package com.tinkerpop.gremlin.tinkergraph.structure.groovy;

import com.tinkerpop.gremlin.groovy.GroovyEnvironmentSuite;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import com.tinkerpop.gremlin.tinkergraph.TinkerGraphGraphProvider;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.runner.RunWith;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GroovyEnvironmentSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = TinkerGraphGraphProvider.class, graph = TinkerGraph.class)
public class TinkerGraphGroovyEnvironmentTest {
    static {
        SugarLoader.load();
    }
}