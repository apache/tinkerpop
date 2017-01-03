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


import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import scala.Tuple2;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Tuple2Serializer<A, B> extends Serializer<Tuple2<A, B>> {

    @Override
    public void write(final Kryo kryo, final Output output, final Tuple2<A, B> tuple2) {
        kryo.writeClassAndObject(output, tuple2._1());
        kryo.writeClassAndObject(output, tuple2._2());
    }

    @Override
    public Tuple2<A, B> read(final Kryo kryo, final Input input, final Class<Tuple2<A, B>> clazz) {
        return new Tuple2(kryo.readClassAndObject(input), kryo.readClassAndObject(input));
    }
}
