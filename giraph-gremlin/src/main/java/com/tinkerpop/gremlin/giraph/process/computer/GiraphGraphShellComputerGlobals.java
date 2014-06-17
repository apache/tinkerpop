package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphShellComputerGlobals implements GraphComputer.Globals {

    final Map<String, Object> globals = new HashMap<>();

    public GiraphGraphShellComputerGlobals(final Configuration configuration) {
        try {
            final String globalLocation = configuration.get(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION, null);
            if (null != globalLocation) {
                for (final String line : this.readLines(new Path(globalLocation + "/" + GiraphGraphRunner.GLOBAL), configuration)) {
                    this.globals.put(line.split("\t")[0], line.split("\t")[1]);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Set<String> keys() {
        return this.globals.keySet();
    }

    public <R> R get(final String key) {
        return (R) this.globals.get(key);
    }

    public void set(final String key, Object value) {
        throw new IllegalStateException("The graph computation is complete and immutable");
    }

    public int getIteration() {
        return Integer.valueOf((String) this.globals.get("iteration"));
    }

    public long getRuntime() {
        return Long.valueOf((String) this.globals.get("runtime"));
    }

    public void setIfAbsent(final String key, final Object value) {
        throw new IllegalStateException("The graph computation is complete and immutable");
    }

    public long incr(final String key, final long delta) {
        throw new IllegalStateException("The graph computation is complete and immutable");
    }

    public boolean and(final String key, final boolean bool) {
        throw new IllegalStateException("The graph computation is complete and immutable");
    }

    public boolean or(final String key, final boolean bool) {
        throw new IllegalStateException("The graph computation is complete and immutable");
    }

    private List<String> readLines(Path location, Configuration conf) throws Exception {
        FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        FileStatus[] items = fileSystem.listStatus(location);
        if (items == null) return new ArrayList<>();
        List<String> results = new ArrayList<>();
        for (FileStatus item : items) {
            // ignoring files like _SUCCESS
            if (item.getPath().getName().startsWith("_")) {
                continue;
            }

            CompressionCodec codec = factory.getCodec(item.getPath());
            InputStream stream = null;

            // check if we have a compression codec we need to use
            if (codec != null) {
                stream = codec.createInputStream(fileSystem.open(item.getPath()));
            } else {
                stream = fileSystem.open(item.getPath());
            }

            StringWriter writer = new StringWriter();
            IOUtils.copy(stream, writer, "UTF-8");
            String raw = writer.toString();
            for (String str : raw.split("\n")) {
                results.add(str);
            }
        }
        return results;
    }
}
