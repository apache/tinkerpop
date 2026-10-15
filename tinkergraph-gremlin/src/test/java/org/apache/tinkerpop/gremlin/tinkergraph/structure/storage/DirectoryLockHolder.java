/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure.storage;

import java.io.File;

/**
 * Holds a {@link DirectoryLock} from a separate JVM so tests can check the lock against another process rather than
 * another channel in their own. Prints {@code locked} once the lock is held and releases it when standard input
 * closes.
 */
public final class DirectoryLockHolder {

    public static void main(final String[] args) throws Exception {
        try (final DirectoryLock lock = DirectoryLock.acquire(new File(args[0]))) {
            System.out.println("locked");
            System.out.flush();
            while (System.in.read() != -1) {
                // wait for the test to close standard input
            }
        }
    }
}
