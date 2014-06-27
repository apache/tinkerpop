package com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.giraph.hdfs.KeyHelper;
import com.tinkerpop.gremlin.giraph.process.JobCreator;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.KryoWritable;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGroupByStep<S, K, V, R> extends FilterStep<S> implements SideEffectCapable, Reversible, VertexCentric, JobCreator {

    private static final String GREMLIN_GROUP_BY_VARIABLE = "gremlin.groupBy.variable";
    private static final String GREMLIN_GROUP_BY_AS = "gremlin.groupBy.as";

    public java.util.Map<K, Collection<V>> groupMap;
    public final java.util.Map<K, R> reduceMap;
    public final SFunction<S, K> keyFunction;
    public final SFunction<S, V> valueFunction;
    public final SFunction<Collection<V>, R> reduceFunction;
    public final String variable;

    public GiraphGroupByStep(final Traversal traversal, final GroupByStep groupByStep) {
        super(traversal);
        this.variable = groupByStep.variable;
        this.reduceMap = new HashMap<>();
        this.keyFunction = groupByStep.keyFunction;
        this.valueFunction = groupByStep.valueFunction == null ? s -> (V) s : groupByStep.valueFunction;
        this.reduceFunction = groupByStep.reduceFunction;
        this.setPredicate(traverser -> {
            doGroup(traverser.get(), this.groupMap, this.keyFunction, this.valueFunction);
            return true;
        });
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.groupMap = vertex.<java.util.Map<K, Collection<V>>>property(Graph.Key.hidden(this.variable)).orElse(new HashMap<>());
        vertex.property(Graph.Key.hidden(this.variable), this.groupMap);
    }

    private static <S, K, V> void doGroup(final S s, final java.util.Map<K, Collection<V>> groupMap, final SFunction<S, K> keyFunction, final SFunction<S, V> valueFunction) {
        final K key = keyFunction.apply(s);
        final V value = valueFunction.apply(s);
        Collection<V> values = groupMap.get(key);
        if (null == values) {
            values = new ArrayList<>();
            groupMap.put(key, values);
        }
        if (value instanceof Iterator) {
            while (((Iterator) value).hasNext()) {
                values.add(((Iterator<V>) value).next());
            }
        } else {
            values.add(value);
        }
    }

    public static class Map extends Mapper<NullWritable, GiraphInternalVertex, Text, KryoWritable> {
        private final Text textWritable = new Text();
        private final KryoWritable kryoWritable = new KryoWritable();
        private String variable;

        @Override
        public void setup(final Mapper<NullWritable, GiraphInternalVertex, Text, KryoWritable>.Context context) {
            this.variable = context.getConfiguration().get(GREMLIN_GROUP_BY_VARIABLE, "null");
        }

        @Override
        public void map(final NullWritable key, final GiraphInternalVertex value, final Mapper<NullWritable, GiraphInternalVertex, Text, KryoWritable>.Context context) throws IOException, InterruptedException {
            final HashMap<Object, Collection> tempMap = value.getGremlinVertex().<HashMap<Object, Collection>>property(Graph.Key.hidden(this.variable)).orElse(new HashMap<>());
            tempMap.forEach((k, v) -> {
                this.textWritable.set(null == k ? "null" : k.toString());
                this.kryoWritable.set(v);
                try {
                    context.write(this.textWritable, this.kryoWritable);
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            });
        }
    }

    public static class Combiner extends Reducer<Text, KryoWritable, Text, KryoWritable> {
        private final KryoWritable kryoWritable = new KryoWritable();

        @Override
        public void reduce(final Text key, final Iterable<KryoWritable> values, final Reducer<Text, KryoWritable, Text, KryoWritable>.Context context) throws IOException, InterruptedException {
            final List list = new ArrayList();
            for (final KryoWritable kryoWritable : values) {
                list.addAll((Collection) kryoWritable.get());
            }
            this.kryoWritable.set(list);
            context.write(key, this.kryoWritable);
        }
    }

    public static class Reduce extends Reducer<Text, KryoWritable, Text, KryoWritable> {
        private final KryoWritable kryoWritable = new KryoWritable();
        private SFunction<Collection, Object> reduceFunction;

        @Override
        public void setup(final Reducer<Text, KryoWritable, Text, KryoWritable>.Context context) throws IOException {
            try {
                final Traversal traversal = (Traversal) VertexProgramHelper.deserializeSupplier(ConfUtil.makeApacheConfiguration(context.getConfiguration()), TraversalVertexProgram.TRAVERSAL_SUPPLIER).get();
                this.reduceFunction = ((GroupByStep) TraversalHelper.getAs(context.getConfiguration().get(GREMLIN_GROUP_BY_AS), traversal)).reduceFunction;
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        @Override
        public void reduce(final Text key, final Iterable<KryoWritable> values, final Reducer<Text, KryoWritable, Text, KryoWritable>.Context context) throws IOException, InterruptedException {
            final List list = new ArrayList();
            for (final KryoWritable kryoWritable : values) {
                list.addAll((Collection) kryoWritable.get());
            }

            if (null == reduceFunction)
                this.kryoWritable.set(list);
            else
                this.kryoWritable.set(this.reduceFunction.apply(list));

            context.write(key, this.kryoWritable);
        }
    }

    public Job createJob(final Configuration configuration) throws IOException {
        final Configuration newConfiguration = new Configuration(configuration);
        newConfiguration.set(GREMLIN_GROUP_BY_VARIABLE, this.variable);
        newConfiguration.set(GREMLIN_GROUP_BY_AS, this.getAs());
        final Job job = new Job(newConfiguration, GiraphGraphComputer.GIRAPH_GREMLIN_JOB_PREFIX + this.toString() + "[SideEffect Calculation]");
        job.setJarByClass(GiraphGraph.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(KryoWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(KryoWritable.class);
        job.setInputFormatClass(ConfUtil.getInputFormatFromVertexInputFormat((Class) newConfiguration.getClass(GiraphGraph.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class)));
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(newConfiguration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + GiraphGraphComputer.G));
        FileOutputFormat.setOutputPath(job, new Path(newConfiguration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION) + "/" + KeyHelper.makeDirectory(this.variable)));
        return job;
    }
}
