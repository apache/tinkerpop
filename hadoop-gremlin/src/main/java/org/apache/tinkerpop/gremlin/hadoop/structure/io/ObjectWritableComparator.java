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
package org.apache.tinkerpop.gremlin.hadoop.structure.io;

import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
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
public abstract class ObjectWritableComparator implements RawComparator<ObjectWritable>, HadoopPoolsConfigurable {

    public static final Logger LOGGER = LoggerFactory.getLogger(ObjectWritableComparator.class);

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

    public static class ObjectWritableMapComparator extends ObjectWritableComparator {
        @Override
        public void setConf(final Configuration configuration) {
            super.setConf(configuration);
            final org.apache.commons.configuration2.Configuration apacheConfiguration = ConfUtil.makeApacheConfiguration(configuration);
            this.comparator = MapReduce.<MapReduce<?,?,?,?,?>>createMapReduce(HadoopGraph.open(apacheConfiguration),apacheConfiguration).getMapKeySort().get();
        }
    }

    public static class ObjectWritableReduceComparator extends ObjectWritableComparator {
        @Override
        public void setConf(final Configuration configuration) {
            super.setConf(configuration);
            final org.apache.commons.configuration2.Configuration apacheConfiguration = ConfUtil.makeApacheConfiguration(configuration);
            this.comparator = MapReduce.<MapReduce<?,?,?,?,?>>createMapReduce(HadoopGraph.open(apacheConfiguration),apacheConfiguration).getReduceKeySort().get();
        }
    }
}
