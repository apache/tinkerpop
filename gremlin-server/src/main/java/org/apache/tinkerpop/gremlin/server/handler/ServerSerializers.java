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
package org.apache.tinkerpop.gremlin.server.handler;

import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
<<<<<<< HEAD
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
=======
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV1d0;
>>>>>>> 3.4-dev

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class ServerSerializers {

    private ServerSerializers() {}

    /**
<<<<<<< HEAD
     * Default binary serializer used by the server when the serializer requested does not match what is on the server.
     * This defaults to GraphBinary 1.0.
=======
     * Default serializer used by the server when the serializer requested does not match what is on the server.
     * Using GraphSON 1.0 on 3.3.5 because that's what it has long been set to in previous versions.
>>>>>>> 3.4-dev
     */
    static final MessageSerializer DEFAULT_BINARY_SERIALIZER = new GraphBinaryMessageSerializerV1();

    /**
     * Default binary serializer used by the server when the serializer requested does not match what is on the server.
     * This defaults to GraphSON 3.0.
     */
    static final MessageSerializer DEFAULT_TEXT_SERIALIZER = new GraphSONMessageSerializerV3d0();

}
