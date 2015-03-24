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

import org.apache.tinkerpop.shaded.kryo_2_24_0.io.Input;
import org.apache.tinkerpop.shaded.kryo_2_24_0.io.Output;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.giraph.GiraphWorkerContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ObjectWritable<T> implements WritableComparable<ObjectWritable> {

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
        return this.t.toString();
    }

    @Override
    public void readFields(final DataInput input) throws IOException {
        this.t = GiraphWorkerContext.GRYO_POOL.doWithReader(gryoReader -> {
            try {
                return gryoReader.readObject(new Input(new ByteArrayInputStream(WritableUtils.readCompressedByteArray(input))));
            } catch (IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        });
    }

    @Override
    public void write(final DataOutput output) throws IOException {
        GiraphWorkerContext.GRYO_POOL.doWithWriter(gryoWriter -> {
            try {
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                final Output out = new Output(outputStream);
                gryoWriter.writeObject(out, this.t);
                out.flush();
                WritableUtils.writeCompressedByteArray(output, outputStream.toByteArray());
                out.close();
            } catch (IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        });
    }

    @Override
    public int compareTo(final ObjectWritable objectWritable) {
        return this.t instanceof Comparable ? ((Comparable) this.t).compareTo(objectWritable.get()) : 0;
    }

    public boolean isEmpty() {
        return null == this.t;
    }

    public static ObjectWritable empty() {
        return new ObjectWritable(null);
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
        return this.isEmpty() ? 0 : this.t.hashCode();
    }
}
