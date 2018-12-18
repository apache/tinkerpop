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
    GRAPH(0X10),
    VERTEX(0X11),
    VERTEXPROPERTY(0X12),
    BARRIER(0X13),
    BINDING(0X14),
    BYTECODE(0X15),
    CARDINALITY(0X16),
    COLUMN(0X17),
    DIRECTION(0X18),
    OPERATOR(0X19),
    ORDER(0X1A),
    PICK(0X1B),
    POP(0X1C),
    LAMBDA(0X1D),
    P(0X1E),
    SCOPE(0X1F),
    T(0X20),
    TRAVERSER(0X21),
    BIGDECIMAL(0X22),
    BIGINTEGER(0X23),
    BYTE(0X24),
    BYTEBUFFER(0X25),
    SHORT(0X26),
    BOOLEAN(0x27),
    TEXTP(0x28),
    TRAVERSALSTRATEGY(0X29),
    BULKSET(0X2A),
    TREE(0X2B),

    CHAR(0X80),
    DURATION(0X81),
    INETADDRESS(0X82),
    INSTANT(0X83),
    LOCALDATE(0X84),
    LOCALDATETIME(0X85),
    LOCALTIME(0X86),
    MONTHDAY(0X87),
    OFFSETDATETIME(0X88),
    OFFSETTIME(0X89),
    PERIOD(0X8A),
    YEAR(0X8B),
    YEARMONTH(0X8C),
    ZONEDATETIME(0X8D),
    ZONEOFFSET(0X8E),

    CUSTOM(0),
    UNSPECIFIED_NULL(0XFE);

    private final int code;
    private static final Map<Integer, DataType> typeByCode = new HashMap<>();

    static {
        for (DataType t : DataType.values()) {
            typeByCode.put(t.code, t);
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
     * Gets a DataType by code.
     */
    public static DataType get(final int code) {
        return typeByCode.get(code);
    }
}
