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
package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;

import java.nio.charset.StandardCharsets;

public class CharSerializer extends SimpleTypeSerializer<Character> {
    public CharSerializer() {
        super(DataType.CHAR);
    }

    @Override
    protected Character readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        final int firstByte = buffer.readByte() & 0xff;
        int byteLength = 1;
        // A byte with the first byte ON (10000000) signals that more bytes are needed to represent the UTF-8 char
        if ((firstByte & 0x80) > 0) {
            if ((firstByte & 0xf0) == 0xf0) { // 0xf0 = 11110000
                byteLength = 4;
            } else if ((firstByte & 0xe0) == 0xe0) { //11100000
                byteLength = 3;
            } else if ((firstByte & 0xc0) == 0xc0) { //11000000
                byteLength = 2;
            }
        }

        byte[] byteArray;
        if (byteLength == 1) {
            byteArray = new byte[] { (byte)firstByte };
        } else {
            byteArray = new byte[byteLength];
            byteArray[0] = (byte)firstByte;
            buffer.readBytes(byteArray, 1, byteLength - 1);
        }

        return new String(byteArray, StandardCharsets.UTF_8).charAt(0);
    }

    @Override
    protected void writeValue(final Character value, final ByteBuf buffer, final GraphBinaryWriter context) throws SerializationException {
        final String stringValue = Character.toString(value);
        buffer.writeBytes(stringValue.getBytes(StandardCharsets.UTF_8));
    }
}
