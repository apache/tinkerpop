package com.tinkerpop.gremlin.console.groovy.plugin;

import com.tinkerpop.gremlin.TestHelper;
import com.tinkerpop.gremlin.groovy.plugin.RemoteException;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        assertThat(acceptor.connect(Arrays.asList(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp").getAbsolutePath())).toString(), startsWith("Connected - "));
    }
}
