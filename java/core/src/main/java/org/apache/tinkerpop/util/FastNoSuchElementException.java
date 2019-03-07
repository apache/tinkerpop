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
package org.apache.tinkerpop.util;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FastNoSuchElementException extends NoSuchElementException {

    private static final long serialVersionUID = 2303108654138257697L;
    private static final FastNoSuchElementException INSTANCE = new FastNoSuchElementException();

    private FastNoSuchElementException() {
    }

    /**
     * Retrieve a singleton, fast {@link NoSuchElementException} without a stack trace.
     */
    public static NoSuchElementException instance() {
        return INSTANCE;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
