package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.process.JobCreator;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GlobalsMapReduce implements JobCreator {

    public static final String GREMLIN_GLOBAL_KEYS = "gremlin.globalKeys";
    public static final String RUNTIME = "runtime";
    public static final String ITERATION = "iteration";

    public static class Map extends Mapper<NullWritable, GiraphVertex, Text, Text> {
        private final Text keyWritable = new Text();
        private final Text valueWritable = new Text();

        @Override
        public void map(final NullWritable key, final GiraphVertex value, final Mapper<NullWritable, GiraphVertex, Text, Text>.Context context) throws IOException, InterruptedException {
            final String[] globalKeys = context.getConfiguration().getStrings(GREMLIN_GLOBAL_KEYS);
            for (final String globalKey : globalKeys) {
                final Property property = value.getGremlinVertex().property(Graph.Key.hidden(globalKey));
                if (property.isPresent()) {
                    this.keyWritable.set(globalKey);
                    this.valueWritable.set(property.value().toString());
                    context.write(this.keyWritable, this.valueWritable);
                }
            }
        }
    }

    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }

    public Job createJob(final Configuration configuration) throws IOException {
        final Job job = new Job(configuration, GiraphGraphComputer.GIRAPH_GREMLIN_JOB_PREFIX + "Globals Calculation");
        job.setJarByClass(GiraphGraph.class);
        job.setMapperClass(GlobalsMapReduce.Map.class);
        job.setCombinerClass(GlobalsMapReduce.Combiner.class);
        job.setReducerClass(GlobalsMapReduce.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(ConfUtil.getInputFormatFromVertexInputFormat((Class) configuration.getClass(GiraphGraphComputer.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class)));
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(configuration.get(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION) + "/" + GiraphGraphComputer.G));
        FileOutputFormat.setOutputPath(job, new Path(configuration.get(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION) + "/" + GiraphGraphComputer.GLOBALS));
        return job;
    }
}
