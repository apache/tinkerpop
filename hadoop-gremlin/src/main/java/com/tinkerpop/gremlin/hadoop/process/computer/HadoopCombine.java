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
package com.tinkerpop.gremlin.hadoop.process.computer;

import com.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import com.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopCombine extends Reducer<ObjectWritable, ObjectWritable, ObjectWritable, ObjectWritable> {

    // TODO: extend HadoopReduce for code reuse

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopCombine.class);
    private MapReduce mapReduce;
    private final HadoopCombineEmitter<ObjectWritable, ObjectWritable> combineEmitter = new HadoopCombineEmitter<>();

    private HadoopCombine() {

    }

    @Override
    public void setup(final Reducer<ObjectWritable, ObjectWritable, ObjectWritable, ObjectWritable>.Context context) {
        this.mapReduce = MapReduce.createMapReduce(ConfUtil.makeApacheConfiguration(context.getConfiguration()));
    }

    @Override
    public void reduce(final ObjectWritable key, final Iterable<ObjectWritable> values, final Reducer<ObjectWritable, ObjectWritable, ObjectWritable, ObjectWritable>.Context context) throws IOException, InterruptedException {
        final Iterator<ObjectWritable> itty = values.iterator();
        this.combineEmitter.setContext(context);
        this.mapReduce.combine(key.get(), new Iterator() {
            @Override
            public boolean hasNext() {
                return itty.hasNext();
            }

            @Override
            public Object next() {
                return itty.next().get();
            }
        }, this.combineEmitter);
    }


    public class HadoopCombineEmitter<OK, OV> implements MapReduce.ReduceEmitter<OK, OV> {

        private Reducer<ObjectWritable, ObjectWritable, ObjectWritable, ObjectWritable>.Context context;
        private final ObjectWritable<OK> keyWritable = new ObjectWritable<>();
        private final ObjectWritable<OV> valueWritable = new ObjectWritable<>();

        public void setContext(final Reducer<ObjectWritable, ObjectWritable, ObjectWritable, ObjectWritable>.Context context) {
            this.context = context;
        }

        @Override
        public void emit(final OK key, final OV value) {
            this.keyWritable.set(key);
            this.valueWritable.set(value);
            try {
                this.context.write(this.keyWritable, this.valueWritable);
            } catch (final Exception e) {
                LOGGER.error(e.getMessage());
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }
}
