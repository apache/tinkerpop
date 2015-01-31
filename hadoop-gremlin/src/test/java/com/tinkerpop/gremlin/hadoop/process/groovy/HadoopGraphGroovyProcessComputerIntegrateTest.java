package com.tinkerpop.gremlin.hadoop.process.groovy;

import com.tinkerpop.gremlin.hadoop.HadoopGraphProvider;
import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.process.GroovyProcessComputerSuite;
import com.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.runner.RunWith;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GroovyProcessComputerSuite.class)
@ProcessComputerSuite.GraphProviderClass(provider = HadoopGraphProvider.class, graph = HadoopGraph.class)
public class HadoopGraphGroovyProcessComputerIntegrateTest {
}
