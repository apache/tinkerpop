package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.GiraphGraphProvider;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.lambda.LambdaVertexProgram;
import com.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputerIntegrateTest {

    @Test
    public void shouldNotDeriveMemoryIfToldSo() throws Exception {
        final GiraphGraphProvider provider = new GiraphGraphProvider();
        final Map<String, Object> map = provider.getBaseConfiguration("gremlin.giraph", GiraphGraphComputerIntegrateTest.class, "shouldNotDeriveMemoryIfToldSo");
        map.put(Constants.GREMLIN_GIRAPH_DERIVE_MEMORY, false);
        final GiraphGraph g = (GiraphGraph) GraphFactory.open(map);
        provider.loadGraphData(g, LoadGraphWith.GraphData.CLASSIC);
        final ComputerResult result = g.compute().program(LambdaVertexProgram.build().
                setup(memory -> {
                }).
                execute((vertex, messenger, memory) -> {
                }).
                terminate(memory -> memory.getIteration() > 2).create()).submit().get();
        assertEquals(result.memory().getIteration(), -1);
    }
}
