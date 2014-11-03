package com.tinkerpop.gremlin.giraph.hdfs;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.structure.Element;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public abstract class GiraphElementIterator<E extends Element> implements Iterator<E> {

    protected final GiraphGraph graph;
    protected final Queue<VertexReader> readers = new LinkedList<>();

    public GiraphElementIterator(final GiraphGraph graph, final VertexInputFormat inputFormat, final Path path) throws IOException {
        this.graph = graph;
        final Configuration configuration = ConfUtil.makeHadoopConfiguration(this.graph.configuration());
        for (final FileStatus status : FileSystem.get(configuration).listStatus(path, HiddenFileFilter.instance())) {
            this.readers.add(inputFormat.createVertexReader(new FileSplit(status.getPath(), 0, Integer.MAX_VALUE, new String[]{}), new TaskAttemptContext(configuration, new TaskAttemptID())));
        }
    }

    public GiraphElementIterator(final GiraphGraph graph) throws IOException {
        try {
            this.graph = graph;
            if (this.graph.configuration().containsKey(Constants.GREMLIN_GIRAPH_INPUT_LOCATION)) {
                final Configuration configuration = ConfUtil.makeHadoopConfiguration(this.graph.configuration());
                final VertexInputFormat inputFormat = this.graph.configuration().getInputFormat().getConstructor().newInstance();
                for (final FileStatus status : FileSystem.get(configuration).listStatus(new Path(graph.configuration().getInputLocation()), HiddenFileFilter.instance())) {
                    this.readers.add(inputFormat.createVertexReader(new FileSplit(status.getPath(), 0, Integer.MAX_VALUE, new String[]{}), new TaskAttemptContext(configuration, new TaskAttemptID())));
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
