package com.tinkerpop.gremlin.giraph.process.graph.step.map;

import com.tinkerpop.gremlin.giraph.GiraphGraphProvider;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.io.kryo.KryoInputFormat;
import org.apache.commons.configuration.MapConfiguration;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpStepTest {

    @Test
    @Ignore
    public void shouldNotInfinitelyHang() {
        Map<String, Object> configuration = new GiraphGraphProvider().getBaseConfiguration(null);
        configuration.put("gremlin.inputLocation", KryoInputFormat.class.getResource("grateful-dead-vertices.gio").getPath());
        GiraphGraph g = GiraphGraph.open(new MapConfiguration(configuration));
        //System.out.println(g.V().out().out().out().count().submit(g.compute()).next());
        //System.out.println(g.V().as("x").out().jump("x", t -> t.getLoops() < 3).submit(g.compute()).count().next());
        g.V().as("x").out().jump("x", t -> t.getLoops() < 3).count().submit(g.compute()).forEach(System.out::println);
    }
}
