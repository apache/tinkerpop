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
package org.apache.tinkerpop.gremlin.driver.ser;

import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;

/**
 * The standard Gryo serializer that uses "detached" graph elements during serialization. Detached elements such as
 * {@link DetachedVertex} include the label and the properties associated with it which could be more costly for
 * network serialization purposes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.4.3, replaced by {@link GraphBinaryMessageSerializerV1}.
 */
@Deprecated
public final class GryoMessageSerializerV1d0 extends AbstractGryoMessageSerializerV1d0 {

    private static final String MIME_TYPE = SerTokens.MIME_GRYO_V1D0;
    private static final String MIME_TYPE_STRINGD = SerTokens.MIME_GRYO_V1D0 + "-stringd";

    /**
     * Creates an instance with a standard {@link GryoMapper} instance.
     */
    public GryoMessageSerializerV1d0() {
        super(GryoMapper.build().version(GryoVersion.V1_0).create());
    }

    /**
     * Creates an instance with a standard {@link GryoMapper} instance. Note that the instance created by the supplied
     * builder will be overridden by {@link #configure} if it is called.
     */
    public GryoMessageSerializerV1d0(final GryoMapper.Builder kryo) {
        super(kryo.version(GryoVersion.V1_0).create());
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{serializeToString ? MIME_TYPE_STRINGD : MIME_TYPE};
    }
}
