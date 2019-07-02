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
package org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim;

import org.apache.commons.configuration2.Configuration;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * This interface exists to decouple HadoopPools from TinkerPop's shaded Kryo.
 * <p>
 * VertexWritable and ObjectWritable formerly implemented Serializable by
 * resorting to statically-pooled shaded Kryo instances maintained by the HadoopPools class.
 * This is awkward because those shaded Kryo instances require class registration by default.
 * <p>
 * Consider what happens with custom property datatypes reachable from the reference graph rooted at an ObjectWritable
 * or VertexWritable instance.  It is not enough for these property classes to merely implement
 * Serializable, though one think that from skimming ObjectWritable/VertexWritable.  Those classes
 * must also register with TinkerPop's internal, shaded Kryo instances as maintained by HadoopPools,
 * or else configure those instances to accept unregistered classes.
 * Otherwise, TinkerPop's shaded Kryo will refuse to serialize those properties (even though
 * they implement Serializable, and even though the user might think they are only using
 * Java's standard Serialization mechanism!).
 * <p>
 * By hiding the mechanics of serialization behind this interface instead of hardcoding it in
 * HadoopPools, the user can decide how to implement serialization for ObjectWritable/VertexWritable
 * (and whatever other classes in TinkerPop decide to implement Serializable but then delegate
 * all of the implementation details, like ObjectWritable/VertexWritable do now).
 */
public interface KryoShimService {

    /**
     * Deserializes an object from an input stream.
     *
     * @param source the stream from which to read an object's serialized form
     * @return the first deserialized object available from {@code source}
     */
    public Object readClassAndObject(final InputStream source);

    /**
     * Serializes an object to an output stream.  This may flush the output stream.
     *
     * @param o    the object to serialize
     * @param sink the stream into which the serialized object is written
     */
    public void writeClassAndObject(final Object o, final OutputStream sink);

    /**
     * Returns this service's relative priority number.  Unless explicitly overridden through a
     * system property ({@link KryoShimServiceLoader#KRYO_SHIM_SERVICE}),
     * the service implementation with the numerically highest priority will be used
     * and all others ignored.  In other words, the highest priority wins (in the absence of a
     * system property override).
     * <p>
     * TinkerPop's current default implementation uses priority value zero.
     * <p>
     * Third-party implementations of this interface should (but are not technically required)
     * to use a priority value with absolute value greater than 100.
     * <p>
     * The implementation currently breaks priority ties by lexicographical comparison of
     * fully-qualified package-and-classname, but this tie-breaking behavior should be
     * considered undefined and subject to future change.  Ties are ignored if the service
     * is explicitly set through the system property mentioned above.
     *
     * @return this implementation's priority value
     */
    public int getPriority();

    /**
     * Attempt to incorporate the supplied configuration in future read/write calls.
     * This method is not guaranteed to have any effect on an instance of this interface
     * after {@link #writeClassAndObject(Object, OutputStream)} or {@link #readClassAndObject(InputStream)}
     * has been invoked on that particular instance.
     *
     * @param conf the configuration to apply to this service's internal serializer
     */
    public void applyConfiguration(final Configuration conf);

    /**
     * Release all resources associated with the shim service.
     * This is called on a forced reload or when the {@link KryoShimServiceLoader} is closed.
     */
    public void close();
}
