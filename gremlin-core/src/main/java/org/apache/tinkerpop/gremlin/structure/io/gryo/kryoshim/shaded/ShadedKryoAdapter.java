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
package org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded;

import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShim;
import org.apache.tinkerpop.shaded.kryo.Kryo;

public class ShadedKryoAdapter implements KryoShim<ShadedInputAdapter, ShadedOutputAdapter> {

    private final Kryo shadedKryo;

    public ShadedKryoAdapter(final Kryo shadedKryo) {
        this.shadedKryo = shadedKryo;
    }

    @Override
    public <T> T readObject(final ShadedInputAdapter input, final Class<T> type) {
        return shadedKryo.readObject(input.getShadedInput(), type);
    }

    @Override
    public Object readClassAndObject(final ShadedInputAdapter input) {
        return shadedKryo.readClassAndObject(input.getShadedInput());
    }

    @Override
    public void writeObject(final ShadedOutputAdapter output, final Object object) {
        shadedKryo.writeObject(output.getShadedOutput(), object);
    }

    @Override
    public void writeClassAndObject(final ShadedOutputAdapter output, final Object object) {
        shadedKryo.writeClassAndObject(output.getShadedOutput(), object);
    }

    @Override
    public <T> T readObjectOrNull(final ShadedInputAdapter input, final Class<T> type) {
        return shadedKryo.readObjectOrNull(input.getShadedInput(), type);
    }

    @Override
    public void writeObjectOrNull(final ShadedOutputAdapter output, final Object object, final Class type) {
        shadedKryo.writeObjectOrNull(output.getShadedOutput(), object, type);
    }
}
