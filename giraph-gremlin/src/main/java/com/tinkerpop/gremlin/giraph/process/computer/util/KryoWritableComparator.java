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
public abstract class KryoWritableComparator implements RawComparator<KryoWritable>, Configurable {

    public static final Logger LOGGER = LoggerFactory.getLogger(KryoWritableComparator.class);

    protected Configuration configuration;
    protected Comparator comparator;
    private final KryoWritable kryo1 = new KryoWritable();
    private final KryoWritable kryo2 = new KryoWritable();

    @Override
    public int compare(final KryoWritable kryo1, final KryoWritable kryo2) {
        return this.comparator.compare(kryo1.get(), kryo2.get());
    }

    @Override
    public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
        try {
            this.kryo1.readFields(new DataInputStream(new ByteArrayInputStream(bytes, i, i1)));
            this.kryo2.readFields(new DataInputStream(new ByteArrayInputStream(bytes1, i2, i3)));
            //System.out.println(kryo1 + "<=>" + kryo2 + ":::" + this.comparator.compare(kryo1.get(), kryo2.get()));
            return this.comparator.compare(this.kryo1.get(), this.kryo2.get());
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw new IllegalStateException(e.getMessage());
        }
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    public static class KryoWritableMapComparator extends KryoWritableComparator {
        @Override
        public void setConf(final Configuration configuration) {
            this.configuration = configuration;
            this.comparator = MapReduceHelper.getMapReduce(configuration).getMapKeySort().get();
        }
    }

    public static class KryoWritableReduceComparator extends KryoWritableComparator {
        @Override
        public void setConf(final Configuration configuration) {
            this.configuration = configuration;
            this.comparator = MapReduceHelper.getMapReduce(configuration).getReduceKeySort().get();
        }
    }


}
