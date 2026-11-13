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
package examples.jpms.extensions;

import examples.jpms.extensions.types.CurrencyAmount;
import examples.jpms.extensions.uint.Uint32;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PDTRegistry;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Verifies that a provider-supplied extension can use TinkerPop's PDT extension points when packaged as a named
 * module. This application is neither an embedded graph nor a remote client and does not start a graph or server.
 * Its local GraphBinary round trips exercise two JPMS boundaries:
 * <ul>
 *     <li>{@code Uint32Adapter} proves that gremlin-core can discover a service declared with {@code provides},
 *     even though the adapter's package is not exported.</li>
 *     <li>{@link CurrencyAmount} proves that gremlin-core can read private fields and invoke a private constructor
 *     when the extension module opens that type's package specifically to gremlin-core.</li>
 * </ul>
 * Any failed check throws, so the process exits with a non-zero status.
 */
public final class ExtensionsApp {

    private ExtensionsApp() {}

    public static void main(final String[] args) throws Exception {
        // Exercises uses/provides across named modules. This fails if Uint32Adapter is not discovered.
        final PDTRegistry registry = PDTRegistry.create();
        if (registry.getPrimitiveAdapterByName("examples:Uint32").isEmpty())
            throw new IllegalStateException("the Uint32Adapter service provided by this module was not discovered");

        // The subsequent write reads private fields and the read invokes the private constructor. Both operations
        // require the qualified opens directive in this application's module descriptor.
        registry.register(CurrencyAmount.class);

        final GraphBinaryWriter writer = new GraphBinaryWriter(TypeSerializerRegistry.INSTANCE, registry);
        final GraphBinaryReader reader = new GraphBinaryReader(TypeSerializerRegistry.INSTANCE, registry);

        check(new Uint32(-1), roundTrip(new Uint32(-1), writer, reader), "Uint32 round trip");
        check(new CurrencyAmount("USD", 1250), roundTrip(new CurrencyAmount("USD", 1250), writer, reader),
                "CurrencyAmount round trip");

        final List<Object> mixed = Arrays.asList(new CurrencyAmount("CAD", 300), new Uint32(42), "plain");
        check(mixed, roundTrip(mixed, writer, reader), "round trip of a list holding both types");

        System.out.println("extensions example completed");
    }

    private static Object roundTrip(final Object value, final GraphBinaryWriter writer,
                                    final GraphBinaryReader reader) throws Exception {
        final ByteBuf byteBuf = Unpooled.buffer();
        try {
            final Buffer buffer = new NettyBufferFactory().create(byteBuf);
            writer.write(value, buffer);
            return reader.read(buffer);
        } finally {
            byteBuf.release();
        }
    }

    private static void check(final Object expected, final Object actual, final String what) {
        if (!expected.equals(actual))
            throw new IllegalStateException(what + ": expected " + expected + " but was " + actual);
    }
}
