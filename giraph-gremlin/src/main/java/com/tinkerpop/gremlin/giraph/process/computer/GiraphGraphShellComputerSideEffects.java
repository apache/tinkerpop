package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.process.computer.SideEffects;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphShellComputerSideEffects implements SideEffects {

    private static final String COMPLETE_AND_IMMUTABLE = "The graph computation sideEffects are complete and immutable";

    final Map<String, Object> sideEffects = new HashMap<>();

    public GiraphGraphShellComputerSideEffects(final Configuration configuration) {
        /*try {
            final String globalLocation = configuration.get(GiraphGraph.GREMLIN_OUTPUT_LOCATION, null);
            if (null != globalLocation) {
                for (final String line : this.readLines(new Path(globalLocation + "/" + GiraphGraphComputer.GLOBALS), configuration)) {
                    this.sideEffects.put(line.split("\t")[0], line.split("\t")[1]);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }*/
    }

    public Set<String> keys() {
        return this.sideEffects.keySet();
    }

    public <R> R get(final String key) {
        return (R) this.sideEffects.get(key);
    }

    public void set(final String key, Object value) {
        this.sideEffects.put(key, value);
    }

    public int getIteration() {
        return (Integer) this.sideEffects.get("iteration");
    }

    public long getRuntime() {
        return (Long) this.sideEffects.get("runtime");
    }

    public void setIfAbsent(final String key, final Object value) {
        throw new IllegalStateException();
    }

    public long incr(final String key, final long delta) {
        throw new IllegalStateException(COMPLETE_AND_IMMUTABLE);
    }

    public boolean and(final String key, final boolean bool) {
        throw new IllegalStateException(COMPLETE_AND_IMMUTABLE);
    }

    public boolean or(final String key, final boolean bool) {
        throw new IllegalStateException(COMPLETE_AND_IMMUTABLE);
    }

    /*private List<String> readLines(Path location, Configuration conf) throws Exception {
        final FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
        final CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        final FileStatus[] items = fileSystem.listStatus(location);
        if (items == null) return new ArrayList<>();
        final List<String> results = new ArrayList<>();
        for (final FileStatus item : items) {
            // ignoring files like _SUCCESS
            if (item.getPath().getName().startsWith("_")) {
                continue;
            }

            final CompressionCodec codec = factory.getCodec(item.getPath());
            InputStream stream = null;

            // check if we have a compression codec we need to use
            if (codec != null) {
                stream = codec.createInputStream(fileSystem.open(item.getPath()));
            } else {
                stream = fileSystem.open(item.getPath());
            }

            final StringWriter writer = new StringWriter();
            IOUtils.copy(stream, writer, "UTF-8");
            final String raw = writer.toString();
            for (final String str : raw.split("\n")) {
                results.add(str);
            }
        }
        return results;
    }*/
}
