package com.tinkerpop.gremlin.giraph.process.groovy;

import com.tinkerpop.gremlin.giraph.GiraphGraphProvider;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.GroovyProcessComputerSuite;
import com.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.runner.RunWith;

/**
 * Executes the Standard Gremlin Process Computer Test Suite using GiraphGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GroovyProcessComputerSuite.class)
@ProcessComputerSuite.GraphProviderClass(provider = GiraphGraphProvider.class, graph = GiraphGraph.class)
public class GiraphGraphGroovyProcessComputerIntegrateTest {
}