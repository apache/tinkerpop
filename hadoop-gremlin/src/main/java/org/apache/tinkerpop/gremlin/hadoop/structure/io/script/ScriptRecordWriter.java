/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.hadoop.structure.io.script;

import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.util.ScriptEngineCache;
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
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class ScriptRecordWriter extends RecordWriter<NullWritable, VertexWritable> {

    protected final static String SCRIPT_FILE = "gremlin.hadoop.scriptOutputFormat.script";
    protected final static String SCRIPT_ENGINE = "gremlin.hadoop.scriptOutputFormat.scriptEngine";
    private final static String VERTEX = "vertex";
    private final static String WRITE_CALL = "stringify(" + VERTEX + ")";
    private final static String UTF8 = "UTF-8";
    private final static byte[] NEWLINE;
    private final DataOutputStream out;
    private final ScriptEngine engine;

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
        this.engine = ScriptEngineCache.get(configuration.get(SCRIPT_ENGINE, ScriptEngineCache.DEFAULT_SCRIPT_ENGINE));
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
