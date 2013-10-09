package com.tinkerpop.gremlin.computer;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.computer.ComputeResult;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgramTest extends TestCase {

    public void testGremlinOLAP() {
        Graph g = TinkerFactory.createClassic();
        ComputeResult result =
                g.compute().program(GremlinVertexProgram.create().steps("V", "out", "out","out","count").build()).submit();
        System.out.println(result.getGraphMemory().get("result").toString());

    }
}
