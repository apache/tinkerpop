package com.tinkerpop.gremlin.console.plugin;

import com.tinkerpop.gremlin.driver.Result;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverRemoteAcceptorIntegrationTest extends AbstractGremlinServerIntegrationTest {
    private final Groovysh groovysh = new Groovysh();
    private DriverRemoteAcceptor acceptor;

    @Before
    public void before() {
        acceptor = new DriverRemoteAcceptor(groovysh);
    }

    @After
    public void after() {
        try {
            acceptor.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void shouldConnect() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(generateTempFile(this.getClass(), "remote.yaml"))).toString(), startsWith("Connected - "));
    }

    @Test
    public void shouldConnectAndSubmitSimple() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(generateTempFile(this.getClass(), "remote.yaml"))).toString(), startsWith("Connected - "));
        assertEquals("2", ((Iterator) acceptor.submit(Arrays.asList("1+1"))).next());
        assertEquals("2", ((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).iterator().next().getString());
    }

    @Test
    public void shouldConnectAndSubmitSimpleList() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(generateTempFile(this.getClass(), "remote.yaml"))).toString(), startsWith("Connected - "));
        assertThat(StreamFactory.stream(((Iterator<String>) acceptor.submit(Arrays.asList("[1,2,3,4,5]")))).collect(Collectors.toList()), contains("1", "2", "3", "4", "5"));
        assertThat(((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).stream().map(result -> result.getString()).collect(Collectors.toList()), contains("1", "2", "3", "4", "5"));
    }

    // todo: let's bring some more tests here

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
