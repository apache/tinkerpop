package com.tinkerpop.gremlin.hadoop.process.groovy;

import com.tinkerpop.gremlin.hadoop.HadoopGraphProvider;
import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GroovyProcessStandardSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = HadoopGraphProvider.class, graph = HadoopGraph.class)
public class HadoopGraphGroovyProcessStandardTest {
}
