package com.tinkerpop.gremlin.giraph.structure.io;

import com.tinkerpop.gremlin.giraph.hdfs.HDFSTools;
import com.tinkerpop.gremlin.giraph.hdfs.HiddenFileFilter;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Vertex;
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
public class VertexIterator implements Iterator<Vertex> {

    private final Queue<VertexReader> readers = new LinkedList<>();
    private Vertex nextVertex = null;

    public VertexIterator(final VertexInputFormat inputFormat, final Configuration configuration) {
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

    public Vertex next() {
        try {
            if (this.nextVertex != null) {
                Vertex temp = this.nextVertex;
                this.nextVertex = null;
                return temp;
            } else {
                while (!this.readers.isEmpty()) {
                    if (this.readers.peek().nextVertex())
                        return ((GiraphVertex) this.readers.peek().getCurrentVertex()).getGremlinVertex();
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
                        this.nextVertex = ((GiraphVertex) this.readers.peek().getCurrentVertex()).getGremlinVertex();
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
