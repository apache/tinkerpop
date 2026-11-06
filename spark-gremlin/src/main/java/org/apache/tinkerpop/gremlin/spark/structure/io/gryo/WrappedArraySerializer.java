/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.spark.structure.io.gryo;

import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import scala.collection.immutable.ArraySeq;

/**
 * Serializer for Scala's immutable {@link ArraySeq}. In Scala 2.13 (the Scala version Spark is built against) wrapping
 * a reference array produces an {@code immutable.ArraySeq.ofRef}, which replaced the {@code WrappedArray} used under
 * Scala 2.12.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WrappedArraySerializer<T> extends Serializer<ArraySeq<T>> {

    @Override
    public void write(final Kryo kryo, final Output output, final ArraySeq<T> iterable) {
        output.writeVarInt(iterable.size(), true);
        for (int i = 0; i < iterable.size(); i++) {
            kryo.writeClassAndObject(output, iterable.apply(i));
        }
    }

    @Override
    public ArraySeq<T> read(final Kryo kryo, final Input input, final Class<ArraySeq<T>> aClass) {
        final int size = input.readVarInt(true);
        final Object[] array = new Object[size];
        for (int i = 0; i < size; i++) {
            array[i] = kryo.readClassAndObject(input);
        }
        return new ArraySeq.ofRef<>((T[]) array);
    }
}
