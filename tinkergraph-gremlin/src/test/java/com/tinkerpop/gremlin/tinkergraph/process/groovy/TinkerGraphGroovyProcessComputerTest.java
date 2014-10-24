package com.tinkerpop.gremlin.tinkergraph.process.groovy;

import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.process.GroovyProcessComputerSuite;
import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import com.tinkerpop.gremlin.tinkergraph.TinkerGraphGraphProvider;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.runner.RunWith;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GroovyProcessComputerSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = TinkerGraphGraphProvider.class, graph = TinkerGraph.class)
public class TinkerGraphGroovyProcessComputerTest {
    static {
        SugarLoader.load();
    }
}
