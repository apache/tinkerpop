package com.tinkerpop.gremlin.tinkergraph.process;

import com.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import com.tinkerpop.gremlin.tinkergraph.TinkerGraphGraphProvider;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.runner.RunWith;

/**
 * Executes the Gremlin Process Test Suite using the Groovy flavor of Gremlin process, using TinkerGraph as the test
 * graph database to execute traversals over.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GroovyProcessStandardSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = TinkerGraphGraphProvider.class, graph = TinkerGraph.class)
public class TinkerGraphGroovyProcessStandardTest {
}
