package com.tinkerpop.gremlin.hadoop.structure.hdfs;

import com.tinkerpop.gremlin.hadoop.structure.HadoopEdge;
import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopEdgeIterator extends HadoopElementIterator<Edge> {

    private Iterator<Edge> edgeIterator = Collections.emptyIterator();

    public HadoopEdgeIterator(final HadoopGraph graph, final InputFormat<NullWritable, VertexWritable> inputFormat, final Path path) throws IOException, InterruptedException {
        super(graph, inputFormat, path);
    }

    public HadoopEdgeIterator(final HadoopGraph graph) throws IOException {
        super(graph);
    }

    @Override
    public Edge next() {
        try {
            while (true) {
                if (this.edgeIterator.hasNext())
                    return new HadoopEdge(this.edgeIterator.next(), this.graph);
                if (this.readers.isEmpty())
                    throw FastNoSuchElementException.instance();
                if (this.readers.peek().nextKeyValue()) {
                    this.edgeIterator = this.readers.peek().getCurrentValue().get().iterators().edgeIterator(Direction.OUT);
                } else {
                    this.readers.remove();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            while (true) {
                if (this.edgeIterator.hasNext())
                    return true;
                if (this.readers.isEmpty())
                    return false;
                if (this.readers.peek().nextKeyValue()) {
                    this.edgeIterator = this.readers.peek().getCurrentValue().get().iterators().edgeIterator(Direction.OUT);
                } else {
                    this.readers.remove();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}