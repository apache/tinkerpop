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
package org.apache.tinkerpop.gremlin.server.util;

import io.cucumber.messages.internal.com.google.common.io.Files;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SSLStoreFilesModificationWatcherTest {
    @Test
    public void shouldDetectFileChange() throws IOException {
        File keyStoreFile = TestHelper.generateTempFileFromResource(SSLStoreFilesModificationWatcherTest.class, "/server-key.jks", "");
        File trustStoreFile = TestHelper.generateTempFileFromResource(SSLStoreFilesModificationWatcherTest.class, "/server-trust.jks", "");

        AtomicBoolean modified = new AtomicBoolean(false);
        SSLStoreFilesModificationWatcher watcher = new SSLStoreFilesModificationWatcher(keyStoreFile.getAbsolutePath(), trustStoreFile.getAbsolutePath(), () -> modified.set(true));

        // No modification yet
        watcher.run();
        assertFalse(modified.get());

        // KeyStore file modified
        Files.touch(keyStoreFile);
        watcher.run();
        assertTrue(modified.get());
        modified.set(false);

        // No modification
        watcher.run();
        assertFalse(modified.get());

        // TrustStore file modified
        Files.touch(trustStoreFile);
        watcher.run();
        assertTrue(modified.get());
        modified.set(false);

        // No modification
        watcher.run();
        assertFalse(modified.get());
    }
}
