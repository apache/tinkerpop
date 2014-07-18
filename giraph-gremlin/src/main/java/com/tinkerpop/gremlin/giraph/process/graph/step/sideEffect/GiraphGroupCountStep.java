package com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.giraph.hdfs.HDFSTools;
import com.tinkerpop.gremlin.giraph.hdfs.HiddenFileFilter;
import com.tinkerpop.gremlin.giraph.hdfs.KeyHelper;
import com.tinkerpop.gremlin.giraph.hdfs.TextFileLineIterator;
import com.tinkerpop.gremlin.giraph.process.JobCreator;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.graph.marker.GiraphSideEffectStep;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGroupCountStep<S> extends FilterStep<S> implements SideEffectCapable, Reversible, Bulkable, VertexCentric, JobCreator, GiraphSideEffectStep<Map> {

    private static final String GREMLIN_GROUP_COUNT_VARIABLE = "gremlin.groupCount.variable";

    private java.util.Map<Object, Long> groupCountMap;
    public final SFunction<S, ?> preGroupFunction;
    public Vertex vertex;
    public final String variable;
    private long bulkCount = 1l;

    public GiraphGroupCountStep(final Traversal traversal, final GroupCountStep groupCountStep) {
        super(traversal);
        this.preGroupFunction = groupCountStep.preGroupFunction;
        this.variable = groupCountStep.variable;
        this.setPredicate(traverser -> {
            MapHelper.incr(this.groupCountMap,
                    null == this.preGroupFunction ? traverser.get() : this.preGroupFunction.apply(traverser.get()),
                    this.bulkCount);
            return true;
        });
        if (TraversalHelper.isLabeled(groupCountStep))
            this.setAs(groupCountStep.getAs());
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.groupCountMap = vertex.<java.util.Map<Object, Long>>property(Graph.Key.hidden(this.variable)).orElse(new HashMap<>());
        vertex.property(Graph.Key.hidden(this.variable), this.groupCountMap);
    }

    public static class Map extends Mapper<NullWritable, GiraphInternalVertex, Text, LongWritable> {
        //private HashMap<Object, Long> map = new HashMap<>();
        //private int mapSpillOver = 1000;
        private final Text textWritable = new Text();
        private final LongWritable longWritable = new LongWritable();
        private String variable;

        @Override
        public void setup(final Mapper<NullWritable, GiraphInternalVertex, Text, LongWritable>.Context context) {
            this.variable = context.getConfiguration().get(GREMLIN_GROUP_COUNT_VARIABLE, "null");
        }

        @Override
        public void map(final NullWritable key, final GiraphInternalVertex value, final Mapper<NullWritable, GiraphInternalVertex, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            // TODO: Kryo is serializing the Map<Object,Long> as a Map<Object,Integer>
            final HashMap<Object, Number> tempMap = value.getTinkerVertex().<HashMap<Object, Number>>property(Graph.Key.hidden(this.variable)).orElse(new HashMap<>());
            tempMap.forEach((k, v) -> {
                this.textWritable.set(null == k ? "null" : k.toString());
                this.longWritable.set(v.longValue());
                try {
                    context.write(this.textWritable, this.longWritable);
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
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
        newConfiguration.set(GREMLIN_GROUP_COUNT_VARIABLE, this.variable);

        final Job job = new Job(newConfiguration, GiraphGraphComputer.GIRAPH_GREMLIN_JOB_PREFIX + this.toString() + "[SideEffect Calculation]");
        job.setJarByClass(GiraphGraph.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(ConfUtil.getInputFormatFromVertexInputFormat((Class) newConfiguration.getClass(GiraphGraph.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class)));
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(newConfiguration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + GiraphGraphComputer.G));
        FileOutputFormat.setOutputPath(job, new Path(newConfiguration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + KeyHelper.makeDirectory(this.variable)));
        return job;
    }

    public java.util.Map<Object, Long> getSideEffect(final Configuration configuration) {
        try {
            final HashMap<Object, Long> map = new HashMap<>();
            final FileSystem fs = FileSystem.get(configuration);
            final Iterator<String> itty = new TextFileLineIterator(fs, new LinkedList(HDFSTools.getAllFilePaths(fs, new Path(configuration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + KeyHelper.makeDirectory(this.variable)), new HiddenFileFilter())), Long.MAX_VALUE);
            itty.forEachRemaining(s -> {
                String[] splits = s.split("\t");
                map.put(splits[0], Long.valueOf(splits[1]));
            });
            return map;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public String getVariable() {
        return this.variable;
    }
}
