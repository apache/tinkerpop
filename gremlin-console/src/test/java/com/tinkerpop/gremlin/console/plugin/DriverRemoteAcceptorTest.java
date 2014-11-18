package com.tinkerpop.gremlin.console.plugin;

import com.tinkerpop.gremlin.groovy.plugin.RemoteException;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverRemoteAcceptorTest {

    private final Groovysh groovysh = new Groovysh();
    private DriverRemoteAcceptor acceptor;

    @Before
    public void setUp() {
        acceptor = new DriverRemoteAcceptor(groovysh);
    }

    @After
    public void tearDown() {
        try {
            acceptor.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test(expected = RemoteException.class)
    public void shouldNotConnectWithEmptyArgs() throws Exception {
        acceptor.connect(new ArrayList<>());
    }

    @Test(expected = RemoteException.class)
    public void shouldNotConnectWithTooManyArgs() throws Exception {
        acceptor.connect(Arrays.asList("two", "too", "many"));
    }

    @Test(expected = RemoteException.class)
    public void shouldNotConnectWithInvalidConfigFile() throws Exception {
        acceptor.connect(Arrays.asList("this-isnt-real.yaml"));
    }

    @Test
    public void shouldConnect() throws Exception {
        // there is no gremlin server running for this test, but gremlin-driver lazily connects so this should
        // be ok to just validate that a connection is created
        assertThat(acceptor.connect(Arrays.asList(generateTempFile(this.getClass(), "remote.yaml"))).toString(), startsWith("Connected - "));
    }

    public static String generateTempFile(final Class resourceClass, final String fileName) throws IOException {
        final File temp = File.createTempFile(fileName, ".tmp");
        final FileOutputStream outputStream = new FileOutputStream(temp);
        int data;
        final InputStream inputStream = resourceClass.getResourceAsStream(fileName);
        while ((data = inputStream.read()) != -1) {
            outputStream.write(data);
        }
        outputStream.close();
        inputStream.close();
        return temp.getPath();
    }
}
