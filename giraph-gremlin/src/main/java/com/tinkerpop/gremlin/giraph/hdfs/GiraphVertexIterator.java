package com.tinkerpop.gremlin.giraph.hdfs;

import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Vertex;
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
public class GiraphVertexIterator implements Iterator<Vertex> {

    private final GiraphGraph graph;
    private GiraphVertex nextVertex = null;
    private final Queue<VertexReader> readers = new LinkedList<>();

    public GiraphVertexIterator(final GiraphGraph graph, final VertexInputFormat inputFormat, final Path path) throws IOException {
        this.graph = graph;
        final Configuration configuration = ConfUtil.makeHadoopConfiguration(graph.variables().getConfiguration());
        for (final FileStatus status : FileSystem.get(configuration).listStatus(path, HiddenFileFilter.instance())) {
            this.readers.add(inputFormat.createVertexReader(new FileSplit(status.getPath(), 0, Integer.MAX_VALUE, new String[]{}), new TaskAttemptContext(configuration, new TaskAttemptID())));
        }
    }

    public GiraphVertexIterator(final GiraphGraph graph) throws IOException {
        try {
            this.graph = graph;
            final Configuration configuration = ConfUtil.makeHadoopConfiguration(graph.variables().getConfiguration());
            final VertexInputFormat inputFormat = graph.variables().getConfiguration().getInputFormat().getConstructor().newInstance();
            for (final FileStatus status : FileSystem.get(configuration).listStatus(new Path(graph.variables().getConfiguration().getInputLocation()), HiddenFileFilter.instance())) {
                this.readers.add(inputFormat.createVertexReader(new FileSplit(status.getPath(), 0, Integer.MAX_VALUE, new String[]{}), new TaskAttemptContext(configuration, new TaskAttemptID())));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public GiraphVertex next() {
        try {
            if (this.nextVertex != null) {
                final GiraphVertex temp = this.nextVertex;
                this.nextVertex = null;
                return temp;
            } else {
                while (!this.readers.isEmpty()) {
                    if (this.readers.peek().nextVertex())
                        return new GiraphVertex(((GiraphInternalVertex) this.readers.peek().getCurrentVertex()).getTinkerVertex(), this.graph);
                    else
                        this.readers.remove();
                }
            }
            throw FastNoSuchElementException.instance();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public boolean hasNext() {
        try {
            if (null != this.nextVertex) return true;
            else {
                while (!this.readers.isEmpty()) {
                    if (this.readers.peek().nextVertex()) {
                        this.nextVertex = new GiraphVertex(((GiraphInternalVertex) this.readers.peek().getCurrentVertex()).getTinkerVertex(), this.graph);
                        return true;
                    } else
                        this.readers.remove();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return false;
    }
}