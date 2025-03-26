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
package org.apache.tinkerpop.gremlin.structure.io.binary;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a GraphBinary data type.
 */
public enum DataType {
    INT(0x01),
    LONG(0x02),
    STRING(0X03),
    DATETIME(0X04),
    DOUBLE(0X07),
    FLOAT(0X08),
    LIST(0X09),
    MAP(0X0A),
    SET(0X0B),
    UUID(0X0C),
    EDGE(0X0D),
    PATH(0X0E),
    PROPERTY(0X0F),
    GRAPH(0X10),
    VERTEX(0X11),
    VERTEXPROPERTY(0X12),
    DIRECTION(0X18),
    T(0X20),
    MERGE(0x2e),
    TRAVERSER(0X21),
    BIGDECIMAL(0X22),
    BIGINTEGER(0X23),
    BYTE(0X24),
    BYTEBUFFER(0X25),
    SHORT(0X26),
    BOOLEAN(0x27),
    BULKSET(0X2A),  // todo:
    TREE(0X2B),

    CHAR(0X80),
    DURATION(0X81),

    CUSTOM(0),
    MARKER(0XFD),
    UNSPECIFIED_NULL(0XFE);

    private final int code;
    private static final Map<Integer, DataType> typeByCode = new HashMap<>();
    private static final Map<DataType, byte[]> bufferByDataType = new HashMap<>();

    static {
        for (DataType t : DataType.values()) {
            typeByCode.put(t.code, t);
            bufferByDataType.put(t, new byte[] { (byte)t.code });
        }
    }

    DataType(final int code) {
        this.code = code;
    }

    /**
     * Gets the data type code.
     */
    public int getCode() {
        return code;
    }

    /**
     * Gets the data type code.
     */
    public byte getCodeByte() {
        return (byte) code;
    }

    /**
     * Gets a byte array containing a single byte representing the type code.
     */
    byte[] getDataTypeBuffer() {
        return bufferByDataType.get(this);
    }

    /**
     * Gets a DataType by code.
     */
    public static DataType get(final int code) {
        return typeByCode.get(code);
    }
}
