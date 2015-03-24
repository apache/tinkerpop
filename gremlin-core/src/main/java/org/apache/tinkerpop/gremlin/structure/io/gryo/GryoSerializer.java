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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GryoSerializer<T> extends Serializer<T> {

    public GryoSerializer() {
        super();
    }

    public GryoSerializer(boolean acceptsNull) {
        super(acceptsNull);
    }


    public GryoSerializer(boolean acceptsNull, boolean immutable) {
        super(acceptsNull, immutable);
    }


    public abstract void write(final GryoKryo kryo, final GryoOutput output, final T object);


    public abstract T read(final GryoKryo kryo, final GryoInput input, final Class<T> type);


    @Override
    public final void write(final Kryo kryo, final Output output, final T object) {
        this.write((GryoKryo) kryo, (GryoOutput) output, object);
    }

    @Override
    public final T read(final Kryo kryo, final Input input, final Class<T> type) {
        return this.read((GryoKryo) kryo, (GryoInput) input, type);
    }
}
