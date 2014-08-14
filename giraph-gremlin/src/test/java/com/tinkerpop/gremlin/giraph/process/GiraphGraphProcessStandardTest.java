package com.tinkerpop.gremlin.giraph.process;

import com.tinkerpop.gremlin.giraph.GiraphGraphProvider;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(ProcessStandardSuite.class)
@Ignore
// TODO: Make it so individual tests can be ignored
@ProcessStandardSuite.GraphProviderClass(provider = GiraphGraphProvider.class, graph = GiraphGraph.class)
public class GiraphGraphProcessStandardTest {
}

