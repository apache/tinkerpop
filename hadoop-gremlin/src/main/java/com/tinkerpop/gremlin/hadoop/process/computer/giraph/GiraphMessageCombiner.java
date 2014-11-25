package com.tinkerpop.gremlin.hadoop.process.computer.giraph;

import com.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import com.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import com.tinkerpop.gremlin.process.computer.MessageCombiner;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.LongWritable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphMessageCombiner extends Combiner<LongWritable, ObjectWritable> implements ImmutableClassesGiraphConfigurable {

    private MessageCombiner messageCombiner;
    private ImmutableClassesGiraphConfiguration configuration;

    @Override
    public void combine(final LongWritable vertexIndex, final ObjectWritable originalMessage, final ObjectWritable messageToCombine) {
        originalMessage.set(originalMessage.isEmpty() ?
                messageToCombine.get() :
                this.messageCombiner.combine(originalMessage.get(), messageToCombine.get()));
    }

    @Override
    public ObjectWritable createInitialMessage() {
        return ObjectWritable.empty();
    }

    @Override
    public void setConf(final ImmutableClassesGiraphConfiguration configuration) {
        this.configuration = configuration;
        this.messageCombiner = (MessageCombiner) VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(configuration)).getMessageCombiner().get();
    }

    @Override
    public ImmutableClassesGiraphConfiguration getConf() {
        return this.configuration;
    }
}
