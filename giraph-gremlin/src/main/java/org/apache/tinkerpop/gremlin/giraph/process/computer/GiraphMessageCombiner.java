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
package org.apache.tinkerpop.gremlin.giraph.process.computer;

import org.apache.commons.configuration.Configuration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphMessageCombiner implements org.apache.giraph.combiner.MessageCombiner<ObjectWritable, ObjectWritable>, ImmutableClassesGiraphConfigurable {

    private MessageCombiner messageCombiner;
    private ImmutableClassesGiraphConfiguration configuration;

    @Override
    public void combine(final ObjectWritable vertexIndex, final ObjectWritable originalMessage, final ObjectWritable messageToCombine) {
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
        final Configuration apacheConfiguration = ConfUtil.makeApacheConfiguration(configuration);
        this.messageCombiner = (MessageCombiner) VertexProgram.createVertexProgram(HadoopGraph.open(apacheConfiguration), apacheConfiguration).getMessageCombiner().get();
    }

    @Override
    public ImmutableClassesGiraphConfiguration getConf() {
        return this.configuration;
    }
}
