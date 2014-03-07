package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.giraph.bsp.BspInputFormat;
import org.apache.giraph.bsp.BspOutputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.GraphMapper;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphRunner extends Configured implements Tool {

    private final GiraphConfiguration giraphConfiguration;

    public GiraphGraphRunner(final GiraphConfiguration giraphConfiguration) {
        this.giraphConfiguration = giraphConfiguration;
        //GiraphJob
    }

    public int run(final String[] args) {
        try {
            final Job job = new Job(this.giraphConfiguration, "GiraphGraph Play");
            job.getConfiguration().setInt("mapreduce.job.counters.limit", 512);
            job.getConfiguration().setInt("mapred.job.map.memory.mb", 1024);
            job.getConfiguration().setInt("mapred.job.reduce.memory.mb", 0);
            job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
            job.getConfiguration().setInt("mapred.map.max.attempts", 0);
            //Client.setPingInterval(job.getConfiguration(), 60000 * 5);
            //job.setJarByClass(GiraphGraphComputer.class);
            job.getConfiguration().set("mapred.jar", "target/giraph-gremlin-3.0.0-SNAPSHOT-job.jar");


            job.setNumReduceTasks(0);
            job.setMapperClass(GraphMapper.class);
            job.setInputFormatClass(BspInputFormat.class);
            job.setOutputFormatClass(BspOutputFormat.class);
            job.waitForCompletion(true);

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;
    }

    public static void main(final String[] args) {
        Configuration configuration = new BaseConfiguration();
        configuration.setProperty("giraph.vertexClass", GiraphVertex.class.getName());
        configuration.setProperty("giraph.vertexInputFormatClass", JsonLongDoubleFloatDoubleVertexInputFormat.class.getName());
        configuration.setProperty("giraph.vertexOutputFormatClass", IdWithValueTextOutputFormat.class.getName());
        configuration.setProperty("giraph.maxWorkers", "1");
        configuration.setProperty("giraph.SplitMasterWorker", "false");
        configuration.setProperty("mapred.job.tracker", "localhost:9001");
        configuration.setProperty("giraph.vertex.input.dir", "tiny_graph.txt");
        configuration.setProperty("mapred.output.dir", "output");

        GiraphGraphComputer g = new GiraphGraphComputer();
        g.configuration(configuration).submit();
    }
}
