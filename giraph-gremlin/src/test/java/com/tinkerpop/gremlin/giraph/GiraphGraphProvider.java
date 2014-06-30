package com.tinkerpop.gremlin.giraph;

import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.io.GiraphGremlinInputFormat;
import com.tinkerpop.gremlin.giraph.structure.io.kryo.KryoInputFormat;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphProvider extends AbstractGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName) {
        return new HashMap<String, Object>() {{
            put("gremlin.graph", GiraphGraph.class.getName());
            put("giraph.vertexInputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.kryo.KryoVertexInputFormat");
            put("giraph.vertexOutputFormatClass", "com.tinkerpop.gremlin.giraph.structure.io.kryo.KryoVertexOutputFormat");
            put("giraph.minWorkers", "1");
            put("giraph.maxWorkers", "1");
            put("giraph.SplitMasterWorker", "false");
            //baseConfiguration.setProperty("giraph.localTestMode", "true");
            put("gremlin.extraJobsCalculator", "com.tinkerpop.gremlin.giraph.process.TraversalExtraJobsCalculator");
            put("giraph.zkJar", GiraphGremlinInputFormat.class.getResource("zookeeper-3.3.3.jar").getPath());
            put("gremlin.inputLocation", KryoInputFormat.class.getResource("tinkerpop-classic-vertices.gio").getPath());
            put("gremlin.outputLocation", "giraph-gremlin/target/test-output");
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null)
            g.close();
    }

    @Override
    public void loadGraphData(final Graph g, final LoadGraphWith loadGraphWith) {

    }
}
