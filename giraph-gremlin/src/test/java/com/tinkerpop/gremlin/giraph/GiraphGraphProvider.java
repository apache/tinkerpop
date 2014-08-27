package com.tinkerpop.gremlin.giraph;

import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.io.GiraphGremlinInputFormat;
import com.tinkerpop.gremlin.giraph.structure.io.kryo.KryoInputFormat;
import com.tinkerpop.gremlin.giraph.structure.io.kryo.KryoVertexInputFormat;
import com.tinkerpop.gremlin.giraph.structure.io.kryo.KryoVertexOutputFormat;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphProvider extends AbstractGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName) {
        return new HashMap<String, Object>() {{
            put("gremlin.graph", GiraphGraph.class.getName());
            put(GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.getKey(), KryoVertexInputFormat.class.getCanonicalName());
            put(GiraphConstants.VERTEX_OUTPUT_FORMAT_CLASS.getKey(), KryoVertexOutputFormat.class.getCanonicalName());
            //put(Constants.GREMLIN_MEMORY_OUTPUT_FORMAT_CLASS, TextOutputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_MEMORY_OUTPUT_FORMAT_CLASS, SequenceFileOutputFormat.class.getCanonicalName());
            put(GiraphConstants.MIN_WORKERS, 1);
            put(GiraphConstants.MAX_WORKERS, 1);
            put(GiraphConstants.SPLIT_MASTER_WORKER.getKey(), false);
            //put("giraph.localTestMode", true);
            put(GiraphConstants.ZOOKEEPER_JAR, GiraphGremlinInputFormat.class.getResource("zookeeper-3.3.3.jar").getPath());
            //put(Constants.GREMLIN_INPUT_LOCATION, KryoInputFormat.class.getResource("tinkerpop-classic-vertices.gio").getPath());
            put(Constants.GREMLIN_OUTPUT_LOCATION, "giraph-gremlin/target/test-output");
            put(Constants.GREMLIN_DERIVE_MEMORY, true);
            put(Constants.GREMLIN_JARS_IN_DISTRIBUTED_CACHE, true);
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null)
            g.close();
    }

    @Override
    public void loadGraphData(final Graph g, final LoadGraphWith loadGraphWith) {
        this.loadGraphData(g, loadGraphWith.value());
    }

    public void loadGraphData(final Graph g, final LoadGraphWith.GraphData graphData) {
        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            ((GiraphGraph) g).variables().getConfiguration().setInputLocation(KryoInputFormat.class.getResource("grateful-dead-vertices.gio").getPath());
        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            ((GiraphGraph) g).variables().getConfiguration().setInputLocation(KryoInputFormat.class.getResource("tinkerpop-modern-vertices.gio").getPath());
        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            ((GiraphGraph) g).variables().getConfiguration().setInputLocation(KryoInputFormat.class.getResource("tinkerpop-classic-vertices.gio").getPath());
        } else {
            throw new RuntimeException("Could not load graph with " + graphData);
        }
    }
}
