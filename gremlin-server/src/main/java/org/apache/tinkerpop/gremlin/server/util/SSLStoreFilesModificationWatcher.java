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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * FileWatcher monitoring changes to SSL keyStore/trustStore files.
 * If a keyStore/trustStore file is set to null, it will be ignored.
 * If a keyStore/trustStore file is deleted, it will be considered not modified.
 */
public class SSLStoreFilesModificationWatcher implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SSLStoreFilesModificationWatcher.class);

    private final Path keyStore;
    private final Path trustStore;
    private final Runnable onModificationRunnable;

    private ZonedDateTime lastModifiedTimeKeyStore = null;
    private ZonedDateTime lastModifiedTimeTrustStore = null;

    /**
     * Create a FileWatcher on keyStore/trustStore
     *
     * @param keyStore               path to the keyStore file or null to ignore
     * @param trustStore             path to the trustStore file or null to ignore
     * @param onModificationRunnable function to run when a modification to the keyStore or trustStore is detected
     */
    public SSLStoreFilesModificationWatcher(String keyStore, String trustStore, Runnable onModificationRunnable) {
        // keyStore/trustStore can be null when not specified in gremlin-server Settings
        this.keyStore = keyStore != null ? Paths.get(keyStore) : null;
        this.trustStore = trustStore != null ? Paths.get(trustStore) : null;
        this.onModificationRunnable = onModificationRunnable;

        // Initialize lastModifiedTime
        try {
            if (this.keyStore != null) {
                lastModifiedTimeKeyStore = getLastModifiedTime(this.keyStore);
            }
            if (this.trustStore != null) {
                lastModifiedTimeTrustStore = getLastModifiedTime(this.trustStore);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        logger.info("Started listening to modifications to the KeyStore and TrustStore files");
    }

    @Override
    public void run() {
        try {
            boolean keyStoreUpdated = false;
            boolean trustStoreUpdated = false;
            ZonedDateTime keyStoreModificationDateTime = null;
            ZonedDateTime trustStoreModificationDateTime = null;

            // Check if the keyStore file still exists and compare its last_modified_time
            if (keyStore != null && Files.exists(keyStore)) {
                keyStoreModificationDateTime = getLastModifiedTime(keyStore);
                keyStoreUpdated = lastModifiedTimeKeyStore.isBefore(keyStoreModificationDateTime);
                if (keyStoreUpdated) {
                    logger.info("KeyStore file has been modified.");
                }
            }

            // Check if the trustStore file still exists and compare its last_modified_time
            if (trustStore != null && Files.exists(trustStore)) {
                trustStoreModificationDateTime = getLastModifiedTime(trustStore);
                trustStoreUpdated = lastModifiedTimeTrustStore.isBefore(trustStoreModificationDateTime);
                if (trustStoreUpdated) {
                    logger.info("TrustStore file has been modified.");
                }
            }

            // If one of the files was updated, execute
            if (keyStoreUpdated || trustStoreUpdated) {
                onModificationRunnable.run();

                if (keyStoreUpdated) {
                    lastModifiedTimeKeyStore = keyStoreModificationDateTime;
                    logger.info("Updated KeyStore configuration");
                }
                if (trustStoreUpdated) {
                    lastModifiedTimeTrustStore = trustStoreModificationDateTime;
                    logger.info("Updated TrustStore configuration");
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ZonedDateTime getLastModifiedTime(Path filepath) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(filepath, BasicFileAttributes.class);
        return ZonedDateTime.ofInstant(attributes.lastModifiedTime().toInstant(), ZoneOffset.UTC);
    }
}