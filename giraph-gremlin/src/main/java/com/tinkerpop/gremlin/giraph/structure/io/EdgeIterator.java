package com.tinkerpop.gremlin.giraph.structure.io;

import com.google.common.collect.Iterators;
import com.tinkerpop.gremlin.giraph.hdfs.HDFSTools;
import com.tinkerpop.gremlin.giraph.hdfs.HiddenFileFilter;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Edge;
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
public class EdgeIterator implements Iterator<Edge> {

    private final Queue<VertexReader> readers = new LinkedList<>();
    private Iterator<Edge> edgeIterator = Iterators.emptyIterator();

    public EdgeIterator(final VertexInputFormat inputFormat, final Configuration configuration) {
        try {
            final String graphPath = configuration.get(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION) + "/" + GiraphGraphComputer.G;
            HDFSTools.getAllFilePaths(FileSystem.get(configuration), new Path(graphPath), new HiddenFileFilter()).forEach(path -> {
                try {
                    this.readers.add(inputFormat.createVertexReader(new FileSplit(path, 0, Integer.MAX_VALUE, new String[]{}), new TaskAttemptContext(new Configuration(), new TaskAttemptID())));
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Edge next() {
        try {
            while (true) {
                if (this.edgeIterator.hasNext())
                    return this.edgeIterator.next();
                if (this.readers.isEmpty())
                    throw FastNoSuchElementException.instance();
                if (this.readers.peek().nextVertex()) {
                    this.edgeIterator = ((GiraphInternalVertex) this.readers.peek().getCurrentVertex()).getGremlinVertex().outE();
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
                    this.edgeIterator = ((GiraphInternalVertex) this.readers.peek().getCurrentVertex()).getGremlinVertex().outE();
                } else {
                    this.readers.remove();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
