package com.tinkerpop.gremlin.giraph.structure.io;

import com.tinkerpop.gremlin.giraph.hdfs.HDFSTools;
import com.tinkerpop.gremlin.giraph.hdfs.HiddenFileFilter;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
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
public class VertexIterator implements Iterator<GiraphVertex> {

    private final Queue<VertexReader> readers = new LinkedList<>();
    private GiraphVertex nextVertex = null;
    private final GiraphGraph graph;

    public VertexIterator(final GiraphGraph graph) {
        this.graph = graph;
        try {
            final Configuration configuration = ConfUtil.makeHadoopConfiguration(this.graph.variables().getConfiguration());
            final VertexInputFormat inputFormat = (VertexInputFormat) configuration.getClass(GiraphGraph.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class).getConstructor().newInstance();
            HDFSTools.getAllFilePaths(FileSystem.get(configuration), new Path(configuration.get(GiraphGraph.GREMLIN_INPUT_LOCATION)), new HiddenFileFilter()).forEach(path -> {
                try {
                    this.readers.add(inputFormat.createVertexReader(new FileSplit(path, 0, Integer.MAX_VALUE, new String[]{}), new TaskAttemptContext(new Configuration(), new TaskAttemptID())));
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            // TODO: This is really weird
            // e.printStackTrace();
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
                        return new GiraphVertex(((GiraphInternalVertex) this.readers.peek().getCurrentVertex()).getGremlinVertex(), this.graph);
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
                        this.nextVertex = new GiraphVertex(((GiraphInternalVertex) this.readers.peek().getCurrentVertex()).getGremlinVertex(), this.graph);
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
