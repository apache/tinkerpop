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
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphStoreStep<S> extends FilterStep<S> implements SideEffectCapable, Reversible, Bulkable, VertexCentric, JobCreator, GiraphSideEffectStep<Collection> {

    private static final String GREMLIN_STORE_VARIABLE = "gremlin.store.variable";

    public SFunction<S, ?> preStoreFunction;
    public String variable;
    protected Collection collection;
    protected long bulkCount = 1l;

    public GiraphStoreStep(final Traversal traversal) {
        super(traversal);
        this.preStoreFunction = null;
        this.variable = null;
    }

    public GiraphStoreStep(final Traversal traversal, final StoreStep storeStep) {
        super(traversal);
        this.preStoreFunction = storeStep.preStoreFunction;
        this.variable = storeStep.variable;
        this.setPredicate(traverser -> {
            final Object storeObject = null == this.preStoreFunction ? traverser.get() : this.preStoreFunction.apply(traverser.get());
            for (int i = 0; i < this.bulkCount; i++) {
                this.collection.add(storeObject);
            }
            return true;
        });
        if (TraversalHelper.isLabeled(storeStep))
            this.setAs(storeStep.getAs());
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.collection = vertex.<Collection>property(Graph.Key.hidden(this.variable)).orElse(new ArrayList());
        vertex.property(Graph.Key.hidden(this.variable), this.collection);
    }

    public static class Map extends Mapper<NullWritable, GiraphInternalVertex, NullWritable, Text> {
        private final Text textWritable = new Text();
        private String variable;

        @Override
        public void setup(final Mapper<NullWritable, GiraphInternalVertex, NullWritable, Text>.Context context) {
            this.variable = context.getConfiguration().get(GREMLIN_STORE_VARIABLE, "null");
        }

        @Override
        public void map(final NullWritable key, final GiraphInternalVertex value, final Mapper<NullWritable, GiraphInternalVertex, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            for (final Object item : value.getTinkerVertex().<Collection>property(Graph.Key.hidden(this.variable)).orElse(Collections.emptyList())) {
                this.textWritable.set(item.toString());
                context.write(NullWritable.get(), this.textWritable);
            }
        }
    }

    public Job createJob(final Configuration configuration) throws IOException {
        final Configuration newConfiguration = new Configuration(configuration);
        newConfiguration.set(GREMLIN_STORE_VARIABLE, this.variable);

        final Job job = new Job(newConfiguration, GiraphGraphComputer.GIRAPH_GREMLIN_JOB_PREFIX + this.toString() + "[SideEffect Calculation]");
        job.setJarByClass(GiraphGraph.class);
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(ConfUtil.getInputFormatFromVertexInputFormat((Class) newConfiguration.getClass(GiraphGraph.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class)));
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(newConfiguration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + GiraphGraphComputer.G));
        FileOutputFormat.setOutputPath(job, new Path(newConfiguration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + KeyHelper.makeDirectory(this.variable)));
        return job;
    }

    public Collection getSideEffect(final Configuration configuration) {
        try {
            final List list = new ArrayList();
            final FileSystem fs = FileSystem.get(configuration);
            final Iterator<String> itty = new TextFileLineIterator(fs, new LinkedList(HDFSTools.getAllFilePaths(fs, new Path(configuration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + KeyHelper.makeDirectory(this.variable)), new HiddenFileFilter())), Long.MAX_VALUE);
            itty.forEachRemaining(list::add);
            return list;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public String getVariable() {
        return this.variable;
    }
}
