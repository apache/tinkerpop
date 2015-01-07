package com.tinkerpop.gremlin.hadoop.structure.hdfs;

import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.hadoop.structure.HadoopVertex;
import com.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopVertexIterator extends HadoopElementIterator<Vertex> {

    private HadoopVertex nextVertex = null;

    public HadoopVertexIterator(final HadoopGraph graph, final InputFormat<NullWritable, VertexWritable> inputFormat, final Path path) throws IOException, InterruptedException {
        super(graph, inputFormat, path);
    }

    public HadoopVertexIterator(final HadoopGraph graph) throws IOException {
        super(graph);
    }

    @Override
    public Vertex next() {
        try {
            if (this.nextVertex != null) {
                final Vertex temp = this.nextVertex;
                this.nextVertex = null;
                return temp;
            } else {
                while (!this.readers.isEmpty()) {
                    if (this.readers.peek().nextKeyValue())
                        return new HadoopVertex(this.readers.peek().getCurrentValue().get(), this.graph);
                    else
                        this.readers.remove();
                }
            }
            throw FastNoSuchElementException.instance();
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            if (null != this.nextVertex) return true;
            else {
                while (!this.readers.isEmpty()) {
                    if (this.readers.peek().nextKeyValue()) {
                        this.nextVertex = new HadoopVertex(this.readers.peek().getCurrentValue().get(), this.graph);
                        return true;
                    } else
                        this.readers.remove();
                }
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return false;
    }
}