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

import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.OutputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;

import java.util.Map;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResponseMessageGryoSerializer implements SerializerShim<ResponseMessage> {
    @Override
    public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final ResponseMessage responseMessage) {
        kryo.writeObjectOrNull(output, responseMessage.getRequestId() != null ? responseMessage.getRequestId() : null, UUID.class);

        // status
        output.writeShort((short) responseMessage.getStatus().getCode().getValue());
        output.writeString(responseMessage.getStatus().getMessage());
        kryo.writeClassAndObject(output, responseMessage.getStatus().getAttributes());

        // result
        kryo.writeClassAndObject(output, responseMessage.getResult().getData());
        kryo.writeClassAndObject(output, responseMessage.getResult().getMeta());

    }

    @Override
    public <I extends InputShim> ResponseMessage read(final KryoShim<I, ?> kryo, final I input, final Class<ResponseMessage> clazz) {
        final UUID requestId = kryo.readObjectOrNull(input, UUID.class);
        final int status = input.readShort();
        final String statusMsg = input.readString();
        final Map<String,Object> statusAttributes = (Map<String,Object>) kryo.readClassAndObject(input);
        final Object result = kryo.readClassAndObject(input);
        final Map<String,Object> metaAttributes = (Map<String,Object>) kryo.readClassAndObject(input);

        return ResponseMessage.build(requestId)
                .code(ResponseStatusCode.getFromValue(status))
                .statusMessage(statusMsg)
                .statusAttributes(statusAttributes)
                .result(result)
                .responseMetaData(metaAttributes)
                .create();
    }
}