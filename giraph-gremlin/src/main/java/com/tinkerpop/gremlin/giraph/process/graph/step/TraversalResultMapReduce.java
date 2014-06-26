package com.tinkerpop.gremlin.giraph.process.graph.step;

import com.tinkerpop.gremlin.giraph.process.JobCreator;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalCounters;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
public class TraversalResultMapReduce implements JobCreator {

    public static class Map extends Mapper<NullWritable, GiraphInternalVertex, Text, LongWritable> {
        private final Text textWritable = new Text();
        private final LongWritable longWritable = new LongWritable();

        /*@Override
        public void setup(final Mapper<NullWritable, GiraphVertex, Text, LongWritable>.Context context) {
            this.variable = context.getConfiguration().get(GREMLIN_GROUP_COUNT_VARIABLE, "null");
        }*/

        @Override
        public void map(final NullWritable key, final GiraphInternalVertex value, final Mapper<NullWritable, GiraphInternalVertex, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            //TODO: determine track paths
            StreamFactory.stream(value.getGremlinVertex())
                    .map(v -> v.<TraversalCounters>property(TraversalVertexProgram.TRAVERSER_TRACKER).orElse(null))
                    .filter(tracker -> null != tracker)
                    .forEach(tracker -> {
                        tracker.getDoneObjectTracks().entrySet().stream().forEach(entry -> {
                            try {
                                this.textWritable.set(entry.getKey().toString());
                                this.longWritable.set(Long.valueOf(entry.getValue().toString()));
                                context.write(this.textWritable, this.longWritable);
                            } catch (Exception e) {
                                throw new RuntimeException(e.getMessage(), e);
                            }
                        });
                        tracker.getDoneGraphTracks().entrySet().stream().forEach(entry -> {
                            try {
                                this.textWritable.set(entry.getKey().toString());
                                this.longWritable.set(Long.valueOf(entry.getValue().toString()));
                                context.write(this.textWritable, this.longWritable);
                            } catch (Exception e) {
                                throw new RuntimeException(e.getMessage(), e);
                            }
                        });
                    });
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void reduce(final Text key, final Iterable<LongWritable> values, final Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long totalCount = 0;
            for (final LongWritable token : values) {
                totalCount = totalCount + token.get();
            }
            this.longWritable.set(totalCount);
            context.write(key, this.longWritable);
        }
    }

    public Job createJob(final Configuration configuration) throws IOException {
        final Configuration newConfiguration = new Configuration(configuration);
        final Job job = new Job(newConfiguration, "TraversalResult[SideEffect Calculation]");
        job.setJarByClass(GiraphGraph.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(ConfUtil.getInputFormatFromVertexInputFormat((Class) newConfiguration.getClass(GiraphGraphComputer.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class)));
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(newConfiguration.get(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION)));
        FileOutputFormat.setOutputPath(job, new Path(newConfiguration.get(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION) + "/traversalResult"));
        return job;
    }
}
