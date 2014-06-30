package com.tinkerpop.gremlin.giraph.structure.io;

import com.google.common.collect.Iterators;
import com.tinkerpop.gremlin.giraph.hdfs.HDFSTools;
import com.tinkerpop.gremlin.giraph.hdfs.HiddenFileFilter;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphEdge;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeIterator implements Iterator<GiraphEdge> {

    private final Queue<VertexReader> readers = new LinkedList<>();
    private Iterator<Edge> edgeIterator = Iterators.emptyIterator();
    private final GiraphGraph graph;

    public EdgeIterator(final GiraphGraph graph) {
        this.graph = graph;
        try {
            final Configuration configuration = ConfUtil.makeHadoopConfiguration(this.graph.variables().getConfiguration());
            final VertexInputFormat inputFormat = (VertexInputFormat) configuration.getClass(GiraphGraph.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class).getConstructor().newInstance();
            HDFSTools.getAllFilePaths(FileSystem.get(configuration), new Path(configuration.get(GiraphGraph.GREMLIN_INPUT_LOCATION)), new HiddenFileFilter()).forEach(path -> {
                try {
                    this.readers.add(inputFormat.createVertexReader(new FileSplit(path, 0, Integer.MAX_VALUE, new String[]{}), new TaskAttemptContext(configuration, new TaskAttemptID())));
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            // TODO: This is really weird
            // e.printStackTrace();
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
                    this.edgeIterator = ((GiraphInternalVertex) this.readers.peek().getCurrentVertex()).getTinkerVertex().outE();
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
                    this.edgeIterator = ((GiraphInternalVertex) this.readers.peek().getCurrentVertex()).getTinkerVertex().outE();
                } else {
                    this.readers.remove();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
