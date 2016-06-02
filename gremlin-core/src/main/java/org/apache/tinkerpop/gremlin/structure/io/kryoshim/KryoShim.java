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
package org.apache.tinkerpop.gremlin.structure.io.kryoshim;

/**
 * A minimal {@link org.apache.tinkerpop.shaded.kryo.Kryo}-like abstraction.
 *
 * @param <I> this interface's complementary InputShim
 * @param <O> this interface's complementary OutputShim
 */
public interface KryoShim<I extends InputShim, O extends OutputShim> {

    <T> T readObject(I input, Class<T> type);

    Object readClassAndObject(I input);

    void writeObject(O output, Object object);

    void writeClassAndObject(O output, Object object);

    <T> T readObjectOrNull(I input, Class<T> type);

    void writeObjectOrNull(O output, Object object, Class type);
}
