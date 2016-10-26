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

package org.apache.tinkerpop.gremlin.spark.structure.io.gryo

import org.apache.spark.util.collection.CompactBuffer
import org.apache.tinkerpop.shaded.kryo.Kryo
import org.apache.tinkerpop.shaded.kryo.Serializer
import org.apache.tinkerpop.shaded.kryo.io.Input
import org.apache.tinkerpop.shaded.kryo.io.Output
import scala.reflect.ClassTag

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CompactBufferSerializer<T> extends Serializer<CompactBuffer<T>> {

    /*
    private final ClassTag<T> evidence$1;
    private T element0;
    private T element1;
    private int org$apache$spark$util$collection$CompactBuffer$$curSize;
    private Object otherElements;
     */

    @Override
    public void write(final Kryo kryo, final Output output, final CompactBuffer<T> compactBuffer) {
        kryo.writeClassAndObject(output, compactBuffer.evidence$1);
        kryo.writeClassAndObject(output, compactBuffer.element0);
        kryo.writeClassAndObject(output, compactBuffer.element1);
        output.writeVarInt(compactBuffer.org$apache$spark$util$collection$CompactBuffer$$curSize, true);
        kryo.writeClassAndObject(output, compactBuffer.otherElements);
    }

    @Override
    public CompactBuffer<T> read(Kryo kryo, Input input, Class<CompactBuffer<T>> aClass) {
        final ClassTag<T> classTag = kryo.readClassAndObject(input);
        final CompactBuffer<T> compactBuffer = new CompactBuffer<>(classTag);
        compactBuffer.element0 = kryo.readClassAndObject(input);
        compactBuffer.element1 = kryo.readClassAndObject(input);
        compactBuffer.org$apache$spark$util$collection$CompactBuffer$$curSize = input.readVarInt(true);
        compactBuffer.otherElements = kryo.readClassAndObject(input);
        return compactBuffer;
    }
}
