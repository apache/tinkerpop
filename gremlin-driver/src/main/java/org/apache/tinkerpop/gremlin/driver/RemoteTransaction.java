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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.structure.Transaction;

/**
 * A {@link Transaction} interface for remote connections that combine lifecycle management and synchronous submission.
 *
 * Note: Implementations of this interface are generally <b>NOT</b> thread-safe.
 */
public interface RemoteTransaction extends Transaction, RequestSubmitter {
    /**
     * Returns the server-generated transaction ID, or {@code null} if the transaction has not yet been started via
     * {@link #begin(Class)}.
     *
     * @return the transaction ID, or null if not yet begun
     */
    String getTransactionId();
}
