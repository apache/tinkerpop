package com.tinkerpop.gremlin.hadoop;

import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.TestHelper;
import com.tinkerpop.gremlin.hadoop.process.computer.giraph.GiraphGraphComputer;
import com.tinkerpop.gremlin.hadoop.structure.HadoopEdge;
import com.tinkerpop.gremlin.hadoop.structure.HadoopElement;
import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.hadoop.structure.HadoopGraphVariables;
import com.tinkerpop.gremlin.hadoop.structure.HadoopProperty;
import com.tinkerpop.gremlin.hadoop.structure.HadoopVertex;
import com.tinkerpop.gremlin.hadoop.structure.HadoopVertexProperty;
import com.tinkerpop.gremlin.hadoop.structure.io.kryo.KryoInputFormat;
import com.tinkerpop.gremlin.hadoop.structure.io.kryo.KryoOutputFormat;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONResourceAccess;
import com.tinkerpop.gremlin.structure.io.kryo.KryoResourceAccess;
import com.tinkerpop.gremlin.structure.io.script.ScriptResourceAccess;
import org.apache.commons.configuration.Configuration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
public class HadoopGraphProvider extends AbstractGraphProvider {

    public static Map<String, String> PATHS = new HashMap<>();
    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(HadoopEdge.class);
        add(HadoopElement.class);
        add(HadoopGraph.class);
        add(HadoopGraphVariables.class);
        add(HadoopProperty.class);
        add(HadoopVertex.class);
        add(HadoopVertexProperty.class);
    }};

    static {
        try {
            final List<String> kryoResources = Arrays.asList(
                    "tinkerpop-modern-vertices.gio",
                    "grateful-dead-vertices.gio",
                    "tinkerpop-classic-vertices.gio",
                    "tinkerpop-crew-vertices.gio");
            for (final String fileName : kryoResources) {
                PATHS.put(fileName, TestHelper.generateTempFileFromResource(KryoResourceAccess.class, fileName, "").getAbsolutePath());
            }

            final List<String> graphsonResources = Arrays.asList(
                    "grateful-dead-vertices.ldjson");
            for (final String fileName : graphsonResources) {
                PATHS.put(fileName, TestHelper.generateTempFileFromResource(GraphSONResourceAccess.class, fileName, "").getAbsolutePath());
            }

            final List<String> scriptResources = Arrays.asList(
                    "tinkerpop-classic.txt",
                    "script-input.groovy",
                    "script-output.groovy");
            for (final String fileName : scriptResources) {
                PATHS.put(fileName, TestHelper.generateTempFileFromResource(ScriptResourceAccess.class, fileName, "").getAbsolutePath());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName) {
        return new HashMap<String, Object>() {{
            put("gremlin.graph", HadoopGraph.class.getName());
            put(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, KryoInputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, KryoOutputFormat.class.getCanonicalName());
            //put(Constants.GREMLIN_GIRAPH_MEMORY_OUTPUT_FORMAT_CLASS, TextOutputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_HADOOP_MEMORY_OUTPUT_FORMAT, SequenceFileOutputFormat.class.getCanonicalName());
            put(GiraphConstants.MIN_WORKERS, 1);
            put(GiraphConstants.MAX_WORKERS, 1);
            put(GiraphConstants.SPLIT_MASTER_WORKER.getKey(), false);
            //put("giraph.localTestMode", true);
            put(GiraphConstants.ZOOKEEPER_JAR, GiraphGraphComputer.class.getResource("zookeeper-3.3.3.jar").getPath());
            //put(Constants.GREMLIN_GIRAPH_INPUT_LOCATION, KryoInputFormat.class.getResource("tinkerpop-classic-vertices.gio").getPath());
            put(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, "hadoop-gremlin/target/test-output");
            put(Constants.GREMLIN_HADOOP_DERIVE_MEMORY, true);
            put(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, true);
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null)
            g.close();
    }

    @Override
    public void loadGraphData(final Graph g, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        if (loadGraphWith != null) this.loadGraphDataViaHadoopConfig(g, loadGraphWith.value());
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    public void loadGraphDataViaHadoopConfig(final Graph g, final LoadGraphWith.GraphData graphData) {

        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            ((HadoopGraph) g).configuration().setInputLocation(PATHS.get("grateful-dead-vertices.gio"));
        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            ((HadoopGraph) g).configuration().setInputLocation(PATHS.get("tinkerpop-modern-vertices.gio"));
        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            ((HadoopGraph) g).configuration().setInputLocation(PATHS.get("tinkerpop-classic-vertices.gio"));
        } else if (graphData.equals(LoadGraphWith.GraphData.CREW)) {
            ((HadoopGraph) g).configuration().setInputLocation(PATHS.get("tinkerpop-crew-vertices.gio"));
        } else {
            throw new RuntimeException("Could not load graph with " + graphData);
        }
    }
}
