package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphMap extends Mapper<NullWritable, GiraphInternalVertex, KryoWritable, KryoWritable> {

    private MapReduce mapReduce;

    public GiraphMap() {

    }

    @Override
    public void setup(final Mapper<NullWritable, GiraphInternalVertex, KryoWritable, KryoWritable>.Context context) {
        try {
            this.mapReduce = context.getConfiguration().getClass("MapReduce", MapReduce.class, MapReduce.class).getConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void map(final NullWritable key, final GiraphInternalVertex value, final Mapper<NullWritable, GiraphInternalVertex, KryoWritable, KryoWritable>.Context context) throws IOException, InterruptedException {
        this.mapReduce.map(value.getTinkerVertex(), new GiraphMapEmitter<>(context));
    }

}
