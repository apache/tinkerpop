package com.tinkerpop.gremlin.hadoop.structure.hdfs;

import com.tinkerpop.gremlin.hadoop.Constants;
import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import com.tinkerpop.gremlin.structure.Element;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class HadoopElementIterator<E extends Element> implements Iterator<E> {

    protected final HadoopGraph graph;
    protected final Queue<RecordReader<NullWritable, VertexWritable>> readers = new LinkedList<>();

    public HadoopElementIterator(final HadoopGraph graph, final InputFormat<NullWritable, VertexWritable> inputFormat, final Path path) throws IOException, InterruptedException {
        this.graph = graph;
        final Configuration configuration = ConfUtil.makeHadoopConfiguration(this.graph.configuration());
        for (final FileStatus status : FileSystem.get(configuration).listStatus(path, HiddenFileFilter.instance())) {
            this.readers.add(inputFormat.createRecordReader(new FileSplit(status.getPath(), 0, Integer.MAX_VALUE, new String[]{}), new TaskAttemptContext(configuration, new TaskAttemptID())));
        }
    }

    public HadoopElementIterator(final HadoopGraph graph) throws IOException {
        try {
            this.graph = graph;
            if (this.graph.configuration().containsKey(Constants.GREMLIN_HADOOP_INPUT_LOCATION)) {
                final Configuration configuration = ConfUtil.makeHadoopConfiguration(this.graph.configuration());
                final InputFormat<NullWritable,VertexWritable> inputFormat = this.graph.configuration().getGraphInputFormat().getConstructor().newInstance();
                for (final FileStatus status : FileSystem.get(configuration).listStatus(new Path(graph.configuration().getInputLocation()), HiddenFileFilter.instance())) {
                    this.readers.add(inputFormat.createRecordReader(new FileSplit(status.getPath(), 0, Integer.MAX_VALUE, new String[]{}), new TaskAttemptContext(configuration, new TaskAttemptID())));
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
