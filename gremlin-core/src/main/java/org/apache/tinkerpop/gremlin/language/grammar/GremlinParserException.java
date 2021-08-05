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
package org.apache.tinkerpop.gremlin.language.grammar;

/**
 * This exception is thrown when a parser error is encountered.
 */
public class GremlinParserException extends RuntimeException {

    private static final long serialVersionUID = 13748205730257427L;

    public GremlinParserException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public GremlinParserException(final String message) {
        super(message);
    }

    public GremlinParserException(final int line_number, final int charPositionInLine, final String message) {
        super(new StringBuilder()
                .append("Query parsing failed at line ")
                .append(line_number)
                .append(", character position at ")
                .append(charPositionInLine)
                .append(", error message : ")
                .append(message).toString());
    }
}
