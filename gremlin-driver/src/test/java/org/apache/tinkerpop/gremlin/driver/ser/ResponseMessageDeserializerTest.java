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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.driver.ser.AbstractGraphSONMessageSerializerV2d0.ResponseMessageDeserializer;

public class ResponseMessageDeserializerTest {

    @Test
    public void shouldNotFailOnCreateObjectWithNullMessage() {
        final Map<String, Object> result = new HashMap<>();
        result.put(SerTokens.TOKEN_DATA, null);
        result.put(SerTokens.TOKEN_META, Collections.emptyMap());

        final Map<String, Object> status = new HashMap<>();
        status.put(SerTokens.TOKEN_MESSAGE, null);
        status.put(SerTokens.TOKEN_CODE, 500);
        status.put(SerTokens.TOKEN_ATTRIBUTES, Collections.emptyMap());

        final Map<String, Object> data = new HashMap<>();
        data.put(SerTokens.TOKEN_REQUEST, UUID.randomUUID().toString());
        data.put(SerTokens.TOKEN_RESULT, result);
        data.put(SerTokens.TOKEN_STATUS, status);

        final AbstractObjectDeserializer<ResponseMessage> deserializer = new ResponseMessageDeserializer();
        Assert.assertNotNull(deserializer.createObject(data));
    }

}
