package com.tinkerpop.gremlin.hadoop.process;

import com.tinkerpop.gremlin.hadoop.HadoopGraphProvider;
import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(ProcessStandardSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = HadoopGraphProvider.class, graph = HadoopGraph.class)
public class HadoopGraphProcessStandardTest {

}
