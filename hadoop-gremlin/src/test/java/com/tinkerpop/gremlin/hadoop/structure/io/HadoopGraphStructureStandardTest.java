package com.tinkerpop.gremlin.hadoop.structure.io;

import com.tinkerpop.gremlin.hadoop.HadoopGraphProvider;
import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(StructureStandardSuite.class)
@StructureStandardSuite.GraphProviderClass(provider = HadoopGraphProvider.class, graph = HadoopGraph.class)
public class HadoopGraphStructureStandardTest {
}


