package com.tinkerpop.gremlin.giraph.process.computer.util;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Comparator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GremlinWritableComparator implements RawComparator<GremlinWritable>, Configurable {

    public static final Logger LOGGER = LoggerFactory.getLogger(GremlinWritableComparator.class);

    protected Configuration configuration;
    protected Comparator comparator;
    private final GremlinWritable gremlin1 = new GremlinWritable();
    private final GremlinWritable gremlin2 = new GremlinWritable();

    @Override
    public int compare(final GremlinWritable kryo1, final GremlinWritable kryo2) {
        return this.comparator.compare(kryo1.get(), kryo2.get());
    }

    @Override
    public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
        try {
            this.gremlin1.readFields(new DataInputStream(new ByteArrayInputStream(bytes, i, i1)));
            this.gremlin2.readFields(new DataInputStream(new ByteArrayInputStream(bytes1, i2, i3)));
            //System.out.println(gremlin1 + "<=>" + gremlin2 + ":::" + this.comparator.compare(gremlin1.get(), gremlin2.get()));
            return this.comparator.compare(this.gremlin1.get(), this.gremlin2.get());
        } catch (final Exception e) {
            LOGGER.error(e.getMessage());
            throw new IllegalStateException(e.getMessage());
        }
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    public static class GremlinWritableMapComparator extends GremlinWritableComparator {
        @Override
        public void setConf(final Configuration configuration) {
            this.configuration = configuration;
            this.comparator = MapReduceHelper.getMapReduce(configuration).getMapKeySort().get();
        }
    }

    public static class GremlinWritableReduceComparator extends GremlinWritableComparator {
        @Override
        public void setConf(final Configuration configuration) {
            this.configuration = configuration;
            this.comparator = MapReduceHelper.getMapReduce(configuration).getReduceKeySort().get();
        }
    }


}
