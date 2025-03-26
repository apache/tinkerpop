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
