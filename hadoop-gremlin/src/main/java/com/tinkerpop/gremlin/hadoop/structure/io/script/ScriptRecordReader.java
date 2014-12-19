package com.tinkerpop.gremlin.hadoop.structure.io.script;

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import com.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import javax.script.ScriptEngine;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptRecordReader extends RecordReader<NullWritable, VertexWritable> {

    protected final static String SCRIPT_FILE = "gremlin.hadoop.inputScript";
    private final static String LINE = "line";
    private final static String FACTORY = "factory";
    private final static String READ_CALL = "parse(" + LINE + "," + FACTORY + ")";
    private final ScriptEngine engine = new GremlinGroovyScriptEngine();
    private final LineRecordReader lineRecordReader;
    private VertexWritable vertex;

    public ScriptRecordReader() {
        this.lineRecordReader = new LineRecordReader();
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        this.lineRecordReader.initialize(genericSplit, context);
        final Configuration configuration = context.getConfiguration();
        final FileSystem fs = FileSystem.get(configuration);
        try {
            this.engine.eval(new InputStreamReader(fs.open(new Path(configuration.get(SCRIPT_FILE)))));
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        while (true) {
            if (!this.lineRecordReader.nextKeyValue()) return false;
            try {
                this.engine.put(LINE, this.lineRecordReader.getCurrentValue().toString());
                this.engine.put(FACTORY, new ScriptElementFactory());
                final Vertex v = (Vertex) engine.eval(READ_CALL);
                if (v != null) {
                    this.vertex = new VertexWritable(v);
                    return true;
                }
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public VertexWritable getCurrentValue() {
        return this.vertex;
    }

    @Override
    public float getProgress() throws IOException {
        return this.lineRecordReader.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        this.lineRecordReader.close();
    }

    protected class ScriptElementFactory {

        private final TinkerGraph graph;

        public ScriptElementFactory() {
            this.graph = TinkerGraph.open();
        }

        public Vertex vertex(final Object id) {
            return vertex(id, Vertex.DEFAULT_LABEL);
        }

        public Vertex vertex(final Object id, final String label) {
            final Traversal<Vertex, Vertex> t = graph.V(id);
            return t.hasNext() ? t.next() : graph.addVertex(T.id, id, T.label, label);
        }

        public Edge edge(final Vertex out, final Vertex in) {
            return edge(out, in, Edge.DEFAULT_LABEL);
        }

        public Edge edge(final Vertex out, final Vertex in, final String label) {
            return out.addEdge(label, in);
        }
    }
}
