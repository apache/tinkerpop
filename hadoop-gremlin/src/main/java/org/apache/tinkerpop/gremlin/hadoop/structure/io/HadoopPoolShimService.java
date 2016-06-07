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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimService;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.io.InputStream;
import java.io.OutputStream;

public class HadoopPoolShimService implements KryoShimService {

    public Object readClassAndObject(final InputStream source) {

        Kryo k = null;

        try {
            k = HadoopPools.getGryoPool().takeKryo();

            return k.readClassAndObject(new Input(source));
        } finally {
            if (null != k) {
                HadoopPools.getGryoPool().offerKryo(k);
            }
        }
    }

    public void writeClassAndObject(final Object o, final OutputStream sink) {

        Kryo k = null;

        try {
            k = HadoopPools.getGryoPool().takeKryo();

            final Output output = new Output(sink);

            k.writeClassAndObject(output, o);

            output.flush();
        } finally {
            if (null != k) {
                HadoopPools.getGryoPool().offerKryo(k);
            }
        }
    }

    @Override
    public int getPriority() {
        return 0;
    }

    @Override
    public void applyConfiguration(final Configuration conf) {
        HadoopPools.initialize(conf);
    }
}
