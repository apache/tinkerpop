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

import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;

/**
 * An extension to the MessageSerializer interface that allows a format to be compatible with text-based
 * websocket messages.  This interface is for internal purposes only.  Implementers who have mapper serialization
 * needs should NOT implement this interface as it will not be used.  Gremlin Server only supports plain JSON
 * for text-based requests.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface MessageTextSerializer<M> extends MessageSerializer<M> {
    public String serializeResponseAsString(final ResponseMessage responseMessage) throws SerializationException;

    public String serializeRequestAsString(final RequestMessage requestMessage) throws SerializationException;

    public RequestMessage deserializeRequest(final String msg) throws SerializationException;

    public ResponseMessage deserializeResponse(final String msg) throws SerializationException;
}
