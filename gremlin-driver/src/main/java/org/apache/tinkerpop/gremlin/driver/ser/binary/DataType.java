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
package org.apache.tinkerpop.gremlin.driver.ser.binary;

import java.util.HashMap;
import java.util.Map;

public enum DataType {
    INT(0x01),
    LONG(0x02),
    STRING(0X03),
    DATE(0X04),
    TIMESTAMP(0X05),
    CLASS(0X06),
    DOUBLE(0X07),
    FLOAT(0X08),
    LIST(0X09),
    MAP(0X0A),
    SET(0X0B),
    UUID(0X0C),
    EDGE(0X0D),
    PATH(0X0E),
    PROPERTY(0X0F),
    TINKERGRAPH(0X10),
    VERTEX(0X11),
    VERTEXPROPERTY(0X12),
    BARRIER(0X13),
    BINDING(0X14),
    BYTECODE(0X15),
    UNSPECIFIED_NULL(0xFF);

    private final int code;
    private static final Map<Integer, DataType> typeByCode = new HashMap<>();

    static {
        for (DataType t : DataType.values()) {
            typeByCode.put(t.code, t);
        }
    }

    DataType(int code) {
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
        return (byte)code;
    }

    /**
     * Gets a DataType by code.
     */
    public static DataType get(int code) {
        return typeByCode.get(code);
    }
}
