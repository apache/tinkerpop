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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimServiceLoader;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ObjectWritable<T> implements WritableComparable<ObjectWritable>, Serializable {

    private static final String NULL = "null";
    private static final ObjectWritable<MapReduce.NullObject> NULL_OBJECT_WRITABLE = new ObjectWritable<>(MapReduce.NullObject.instance());

    T t;

    public ObjectWritable() {
    }

    public ObjectWritable(final T t) {
        this.t = t;
    }

    public T get() {
        return this.t;
    }

    public void set(final T t) {
        this.t = t;
    }

    @Override
    public String toString() {
        return null == this.t ? NULL : this.t.toString();
    }

    @Override
    public void readFields(final DataInput input) throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(WritableUtils.readCompressedByteArray(input));
        this.t = KryoShimServiceLoader.readClassAndObject(bais);
    }

    @Override
    public void write(final DataOutput output) throws IOException {
        final byte serialized[] = KryoShimServiceLoader.writeClassAndObjectToBytes(this.t);
        WritableUtils.writeCompressedByteArray(output, serialized);
    }

    private void writeObject(final ObjectOutputStream outputStream) throws IOException {
        this.write(outputStream);
    }

    private void readObject(final ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        this.readFields(inputStream);
    }

    @Override
    public int compareTo(final ObjectWritable objectWritable) {
        if (null == this.t)
            return objectWritable.isEmpty() ? 0 : -1;
        else if (this.t instanceof Comparable && !objectWritable.isEmpty())
            return ((Comparable) this.t).compareTo(objectWritable.get());
        else if (this.t.equals(objectWritable.get()))
            return 0;
        else
            return -1;
    }

    public boolean isEmpty() {
        return null == this.t;
    }

    public static <A> ObjectWritable<A> empty() {
        return new ObjectWritable<>(null);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ObjectWritable))
            return false;
        else if (this.isEmpty())
            return ((ObjectWritable) other).isEmpty();
        else
            return this.t.equals(((ObjectWritable) other).get());
    }

    @Override
    public int hashCode() {
        return null == this.t ? 0 : this.t.hashCode();
    }

    public static ObjectWritable<MapReduce.NullObject> getNullObjectWritable() {
        return NULL_OBJECT_WRITABLE;
    }
}
