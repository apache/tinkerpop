package com.tinkerpop.gremlin.hadoop.structure.io;

import com.tinkerpop.gremlin.hadoop.process.computer.util.MapReduceHelper;
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
public abstract class ObjectWritableComparator implements RawComparator<ObjectWritable>, Configurable {

    public static final Logger LOGGER = LoggerFactory.getLogger(ObjectWritableComparator.class);

    protected Configuration configuration;
    protected Comparator comparator;
    private final ObjectWritable objectWritable1 = new ObjectWritable();
    private final ObjectWritable objectWritable2 = new ObjectWritable();

    @Override
    public int compare(final ObjectWritable objectWritable1, final ObjectWritable objectWritable2) {
        return this.comparator.compare(objectWritable1.get(), objectWritable2.get());
    }

    @Override
    public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
        try {
            this.objectWritable1.readFields(new DataInputStream(new ByteArrayInputStream(bytes, i, i1)));
            this.objectWritable2.readFields(new DataInputStream(new ByteArrayInputStream(bytes1, i2, i3)));
            //System.out.println(objectWritable1 + "<=>" + objectWritable2 + ":::" + this.comparator.compare(objectWritable1.get(), objectWritable2.get()));
            return this.comparator.compare(this.objectWritable1.get(), this.objectWritable2.get());
        } catch (final Exception e) {
            LOGGER.error(e.getMessage());
            throw new IllegalStateException(e.getMessage());
        }
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    public static class ObjectWritableMapComparator extends ObjectWritableComparator {
        @Override
        public void setConf(final Configuration configuration) {
            this.configuration = configuration;
            this.comparator = MapReduceHelper.getMapReduce(configuration).getMapKeySort().get();
        }
    }

    public static class ObjectWritableReduceComparator extends ObjectWritableComparator {
        @Override
        public void setConf(final Configuration configuration) {
            this.configuration = configuration;
            this.comparator = MapReduceHelper.getMapReduce(configuration).getReduceKeySort().get();
        }
    }
}
