package com.tinkerpop.gremlin.giraph.hdfs;

import com.google.common.collect.Iterators;
import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphEdge;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
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
public class GiraphEdgeIterator implements Iterator<Edge> {

    private final GiraphGraph graph;
    private Iterator<Edge> edgeIterator = Iterators.emptyIterator();
    private final Queue<VertexReader> readers = new LinkedList<>();

    public GiraphEdgeIterator(final GiraphGraph graph, final VertexInputFormat inputFormat, final Path path) throws IOException {
        this.graph = graph;
        final Configuration configuration = ConfUtil.makeHadoopConfiguration(graph.variables().getConfiguration());
        for (final FileStatus status : FileSystem.get(configuration).listStatus(path, HiddenFileFilter.instance())) {
            this.readers.add(inputFormat.createVertexReader(new FileSplit(status.getPath(), 0, Integer.MAX_VALUE, new String[]{}), new TaskAttemptContext(configuration, new TaskAttemptID())));
        }
    }

    public GiraphEdgeIterator(final GiraphGraph graph) throws IOException {
        try {
            this.graph = graph;
            if (this.graph.variables().getConfiguration().containsKey(Constants.GREMLIN_INPUT_LOCATION)) {
                final Configuration configuration = ConfUtil.makeHadoopConfiguration(graph.variables().getConfiguration());
                final VertexInputFormat inputFormat = graph.variables().getConfiguration().getInputFormat().getConstructor().newInstance();
                for (final FileStatus status : FileSystem.get(configuration).listStatus(new Path(graph.variables().getConfiguration().getInputLocation()), HiddenFileFilter.instance())) {
                    this.readers.add(inputFormat.createVertexReader(new FileSplit(status.getPath(), 0, Integer.MAX_VALUE, new String[]{}), new TaskAttemptContext(configuration, new TaskAttemptID())));
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public GiraphEdge next() {
        try {
            while (true) {
                if (this.edgeIterator.hasNext())
                    return new GiraphEdge((TinkerEdge) this.edgeIterator.next(), this.graph);
                if (this.readers.isEmpty())
                    throw FastNoSuchElementException.instance();
                if (this.readers.peek().nextVertex()) {
                    this.edgeIterator = ((GiraphInternalVertex) this.readers.peek().getCurrentVertex()).getTinkerVertex().edges(Direction.OUT, Integer.MAX_VALUE);
                } else {
                    this.readers.remove();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public boolean hasNext() {
        try {
            while (true) {
                if (this.edgeIterator.hasNext())
                    return true;
                if (this.readers.isEmpty())
                    return false;
                if (this.readers.peek().nextVertex()) {
                    this.edgeIterator = ((GiraphInternalVertex) this.readers.peek().getCurrentVertex()).getTinkerVertex().edges(Direction.OUT, Integer.MAX_VALUE);
                } else {
                    this.readers.remove();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}