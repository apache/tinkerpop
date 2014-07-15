package com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.giraph.hdfs.HDFSTools;
import com.tinkerpop.gremlin.giraph.hdfs.HiddenFileFilter;
import com.tinkerpop.gremlin.giraph.hdfs.KeyHelper;
import com.tinkerpop.gremlin.giraph.hdfs.TextFileLineIterator;
import com.tinkerpop.gremlin.giraph.process.JobCreator;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphCountStep<S> extends FilterStep<S> implements SideEffectCapable, Bulkable, VertexCentric, JobCreator, GiraphSideEffectStep<Long> {

    private static final String COUNT = Graph.Key.hidden("count");

    public Vertex vertex;
    private long bulkCount = 1l;

    public GiraphCountStep(final Traversal traversal, final CountStep countStep) {
        super(traversal);
        this.setPredicate(traverser -> {
            this.vertex.property(COUNT, this.vertex.<Long>value(COUNT) + this.bulkCount);
            return true;
        });
        if (TraversalHelper.isLabeled(countStep))
            this.setAs(countStep.getAs());
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.vertex = vertex;
        final Long count = vertex.<Long>property(COUNT).orElse(0l);
        this.vertex.property(COUNT, count);
    }

    public static class Map extends Mapper<NullWritable, GiraphInternalVertex, NullWritable, LongWritable> {
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void map(final NullWritable key, final GiraphInternalVertex value, final Mapper<NullWritable, GiraphInternalVertex, NullWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            this.longWritable.set(value.getTinkerVertex().<Long>property(COUNT).orElse(0l));
            context.write(NullWritable.get(), this.longWritable);
        }
    }

    public static class Reduce extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void reduce(final NullWritable key, final Iterable<LongWritable> values, final Reducer<NullWritable, LongWritable, NullWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            long totalCount = 0;
            for (final LongWritable token : values) {
                totalCount = totalCount + token.get();
            }
            this.longWritable.set(totalCount);
            context.write(NullWritable.get(), this.longWritable);
        }
    }

    public Job createJob(final Configuration configuration) throws IOException {
        final Configuration newConfiguration = new Configuration(configuration);

        final Job job = new Job(newConfiguration, GiraphGraphComputer.GIRAPH_GREMLIN_JOB_PREFIX + this.toString() + "[SideEffect Calculation]");
        job.setJarByClass(GiraphGraph.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(ConfUtil.getInputFormatFromVertexInputFormat((Class) newConfiguration.getClass(GiraphGraph.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class)));
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(newConfiguration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + GiraphGraphComputer.G));
        FileOutputFormat.setOutputPath(job, new Path(newConfiguration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + KeyHelper.makeDirectory(COUNT)));
        return job;
    }

    public Long getSideEffect(final Configuration configuration) {
        try {
            final FileSystem fs = FileSystem.get(configuration);
            final Iterator<String> itty = new TextFileLineIterator(fs, new LinkedList(HDFSTools.getAllFilePaths(fs, new Path(configuration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + KeyHelper.makeDirectory(COUNT)), new HiddenFileFilter())), Long.MAX_VALUE);
            AtomicLong count = new AtomicLong(0l);
            itty.forEachRemaining(s -> count.set(count.get() + Long.valueOf(s)));
            return count.get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
