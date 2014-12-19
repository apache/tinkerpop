package com.tinkerpop.gremlin.hadoop.structure.io.script;

import com.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import javax.script.ScriptEngine;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

/**
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
public class ScriptRecordWriter extends RecordWriter<NullWritable, VertexWritable> {

    protected final static String SCRIPT_FILE = "gremlin.hadoop.scriptOutputFormat.script";
    protected final static String SCRIPT_ENGINE = "gremlin.hadoop.scriptOutputFormat.scriptEngine";
    private final static String VERTEX = "vertex";
    private final static String WRITE_CALL = "stringify(" + VERTEX + ")";
    private final static String UTF8 = "UTF-8";
    private final static byte[] NEWLINE;
    private final DataOutputStream out;
    private ScriptEngine engine;

    static {
        try {
            NEWLINE = "\n".getBytes(UTF8);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("Can not find " + UTF8 + " encoding");
        }
    }

    public ScriptRecordWriter(final DataOutputStream out, final TaskAttemptContext context) throws IOException {
        this.out = out;
        final Configuration configuration = context.getConfiguration();
        this.engine = ScriptInputEngineManager.get(configuration.get(SCRIPT_ENGINE, ScriptInputEngineManager.DEFAULT_SCRIPT_ENGINE));
        final FileSystem fs = FileSystem.get(configuration);
        try {
            this.engine.eval(new InputStreamReader(fs.open(new Path(configuration.get(SCRIPT_FILE)))));
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public void write(final NullWritable key, final VertexWritable vertex) throws IOException {
        if (null != vertex) {
            try {
                this.engine.put(VERTEX, vertex.get());
                final String line = (String) engine.eval(WRITE_CALL);
                if (line != null) {
                    out.write(line.getBytes(UTF8));
                    this.out.write(NEWLINE);
                }
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException {
        this.out.close();
    }
}
