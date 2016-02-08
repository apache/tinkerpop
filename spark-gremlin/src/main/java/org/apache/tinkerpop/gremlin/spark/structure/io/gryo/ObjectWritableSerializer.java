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

package org.apache.tinkerpop.gremlin.spark.structure.io.gryo;

import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ObjectWritableSerializer<T> extends Serializer<ObjectWritable<T>> {

    @Override
    public void write(final Kryo kryo, final Output output, final ObjectWritable<T> objectWritable) {
        kryo.writeClassAndObject(output, objectWritable.get());
        output.flush();
    }

    @Override
    public ObjectWritable<T> read(final Kryo kryo, final Input input, final Class<ObjectWritable<T>> clazz) {
        return new ObjectWritable(kryo.readClassAndObject(input));
    }
}
