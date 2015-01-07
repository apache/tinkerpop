package com.tinkerpop.gremlin.console.groovy.plugin;

import com.tinkerpop.gremlin.TestHelper;
import com.tinkerpop.gremlin.driver.Result;
import com.tinkerpop.gremlin.server.Settings;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverRemoteAcceptorIntegrateTest extends AbstractGremlinServerIntegrationTest {
    private final Groovysh groovysh = new Groovysh();
    private DriverRemoteAcceptor acceptor;

    @Rule
    public TestName name = new TestName();

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        try {
            final String tinkerGraphConfig = TestHelper.generateTempFileFromResource(this.getClass(), "tinkergraph-empty.properties", ".tmp").getAbsolutePath();
            settings.graphs.put("g", tinkerGraphConfig);
            return settings;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Before
    public void before() throws Exception {
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
        assertThat(acceptor.connect(Arrays.asList(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp").getAbsolutePath())).toString(), startsWith("Connected - "));
    }

    @Test
    public void shouldConnectAndSubmitSimple() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp").getAbsolutePath())).toString(), startsWith("Connected - "));
        assertEquals("2", ((Iterator) acceptor.submit(Arrays.asList("1+1"))).next());
        assertEquals("2", ((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).iterator().next().getString());
    }

    @Test
    public void shouldConnectAndSubmitSimpleList() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp").getAbsolutePath())).toString(), startsWith("Connected - "));
        assertThat(StreamFactory.stream(((Iterator<String>) acceptor.submit(Arrays.asList("[1,2,3,4,5]")))).collect(Collectors.toList()), contains("1", "2", "3", "4", "5"));
        assertThat(((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).stream().map(Result::getString).collect(Collectors.toList()), contains("1", "2", "3", "4", "5"));
    }

    @Test
    public void shouldConnectAndReturnVertices() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp").getAbsolutePath())).toString(), startsWith("Connected - "));
        assertThat(StreamFactory.stream(((Iterator<String>) acceptor.submit(Arrays.asList("g.addVertex('name','stephen');g.addVertex('name','marko');g.V()")))).collect(Collectors.toList()), hasSize(2));
        assertThat(((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).stream().map(Result::getString).collect(Collectors.toList()), hasSize(2));
    }

    @Test
    public void shouldConnectAndSubmitForNull() throws Exception {
        assertThat(acceptor.connect(Arrays.asList(TestHelper.generateTempFileFromResource(this.getClass(), "remote.yaml", ".tmp").getAbsolutePath())).toString(), startsWith("Connected - "));
        assertThat(StreamFactory.stream(((Iterator<String>) acceptor.submit(Arrays.asList("g.V().remove()")))).collect(Collectors.toList()), contains("null"));
        assertThat(((List<Result>) groovysh.getInterp().getContext().getProperty(DriverRemoteAcceptor.RESULT)).stream().map(Result::getObject).collect(Collectors.toList()), contains("null"));
    }
}
