package com.tinkerpop.gremlin.giraph.hdfs;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertexIterator extends GiraphElementIterator<GiraphVertex> {

    private GiraphVertex nextVertex = null;

    public GiraphVertexIterator(final GiraphGraph graph, final VertexInputFormat inputFormat, final Path path) throws IOException {
        super(graph, inputFormat, path);
    }

    public GiraphVertexIterator(final GiraphGraph graph) throws IOException {
        super(graph);
    }

    @Override
    public GiraphVertex next() {
        try {
            if (this.nextVertex != null) {
                final GiraphVertex temp = this.nextVertex;
                this.nextVertex = null;
                return temp;
            } else {
                while (!this.readers.isEmpty()) {
                    if (this.readers.peek().nextVertex())
                        return new GiraphVertex(((GiraphComputeVertex) this.readers.peek().getCurrentVertex()).getBaseVertex(), this.graph);
                    else
                        this.readers.remove();
                }
            }
            throw FastNoSuchElementException.instance();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            if (null != this.nextVertex) return true;
            else {
                while (!this.readers.isEmpty()) {
                    if (this.readers.peek().nextVertex()) {
                        this.nextVertex = new GiraphVertex(((GiraphComputeVertex) this.readers.peek().getCurrentVertex()).getBaseVertex(), this.graph);
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