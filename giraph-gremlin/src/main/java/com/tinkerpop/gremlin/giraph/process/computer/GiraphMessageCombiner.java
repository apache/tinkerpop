package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.GremlinWritable;
import com.tinkerpop.gremlin.process.computer.MessageCombiner;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.LongWritable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphMessageCombiner extends Combiner<LongWritable, GremlinWritable> implements ImmutableClassesGiraphConfigurable {

    private MessageCombiner messageCombiner;
    private ImmutableClassesGiraphConfiguration configuration;

    @Override
    public void combine(final LongWritable vertexIndex, final GremlinWritable originalMessage, final GremlinWritable messageToCombine) {
        originalMessage.set(originalMessage.isEmpty() ?
                messageToCombine.get() :
                this.messageCombiner.combine(originalMessage.get(), messageToCombine.get()));
    }

    @Override
    public GremlinWritable createInitialMessage() {
        return GremlinWritable.empty();
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
