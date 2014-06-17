package com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.giraph.process.JobCreator;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.FileOnlyPathFilter;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.UnBulkable;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;
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
import java.util.HashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGroupCountStep<S> extends FilterStep<S> implements SideEffectCapable, Reversible, UnBulkable, VertexCentric, JobCreator {

    public static final String GIRAPH_GROUP_COUNT = Property.hidden("giraphGroupCount");

    public java.util.Map<Object, Long> groupCountMap;
    public FunctionRing<S, ?> functionRing;
    public Vertex vertex;

    public GiraphGroupCountStep(final Traversal traversal, final SFunction<S, ?>... preGroupFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing<>(preGroupFunctions);
        this.setPredicate(traverser -> {
            MapHelper.incr(this.groupCountMap, this.functionRing.next().apply(traverser.get()), 1l);
            return true;
        });
    }

    /*public GiraphGroupCountStep(final Traversal traversal, final String variable, final SFunction<S, ?>... preGroupFunctions) {
        this(traversal, preGroupFunctions);
    }*/

    public void setCurrentVertex(final Vertex vertex) {
        this.groupCountMap = vertex.<java.util.Map<Object, Long>>property(GIRAPH_GROUP_COUNT).orElse(new HashMap<>());
        vertex.property(GIRAPH_GROUP_COUNT, this.groupCountMap);
    }

    public static class Map extends Mapper<NullWritable, GiraphVertex, Text, LongWritable> {
        //private HashMap<Object, Long> map = new HashMap<>();
        //private int mapSpillOver = 1000;
        private final Text textWritable = new Text();
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void map(final NullWritable key, final GiraphVertex value, final Mapper<NullWritable, GiraphVertex, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            final HashMap<Object, Integer> tempMap = value.getGremlinVertex().<HashMap<Object, Integer>>property(GiraphGroupCountStep.GIRAPH_GROUP_COUNT).orElse(new HashMap<>());
            tempMap.forEach((k, v) -> {
                this.textWritable.set(null == k ? "null" : k.toString());
                this.longWritable.set((long) v);
                try {
                    context.write(this.textWritable, this.longWritable);
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            });
        }
    }

    public static class Combiner extends Reducer<Text, LongWritable, Text, LongWritable> {
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
        final Job job = new Job(configuration, this.toString() + ":SideEffect");
        job.setJarByClass(GiraphGraph.class);
        job.setMapperClass(GiraphGroupCountStep.Map.class);
        job.setCombinerClass(GiraphGroupCountStep.Combiner.class);
        job.setReducerClass(GiraphGroupCountStep.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setInputFormatClass(ConfUtil.getInputFormatFromVertexInputFormat((Class) configuration.getClass(GiraphGraphComputer.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class)));
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(configuration.get(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION)));
        FileOutputFormat.setOutputPath(job, new Path(configuration.get(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION) + "/groupCountStep"));
        FileInputFormat.setInputPathFilter(job, FileOnlyPathFilter.class);
        return job;
    }
}
