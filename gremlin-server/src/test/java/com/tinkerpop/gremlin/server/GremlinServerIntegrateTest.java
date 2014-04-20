package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Cluster;
import com.tinkerpop.gremlin.driver.Item;
import com.tinkerpop.gremlin.driver.ResultSet;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerIntegrateTest extends AbstractGremlinServerIntegrationTest {

    @Rule
    public TestName name = new TestName();

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldReceiveFailureTimeOutOnScriptEval":
                settings.scriptEvaluationTimeout = 200;
                break;
            case "shouldReceiveFailureTimeOutOnTotalSerialization":
                settings.serializedResponseTimeout = 1;
                break;
            case "shouldReceiveFailureForTimeoutOfIndividualSerialization":
                settings.serializeResultTimeout = 1;
                break;
            case "shouldBlockRequestWhenTooBig":
                settings.maxContentLength = 1;    // todo: get this to work properly
                break;
        }

        return settings;
    }

    @Test
    public void basicTesting() throws Exception {
        final Cluster cluster = Cluster.create("localhost").build();
        final Client client = cluster.connect();

        System.out.println("iterable --- ");
        ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
        while (!results.allItemsAvailable()) {
            System.out.println("waiting for all...");
            Thread.sleep(1000);
        }

        for (Item result : results) {
            System.out.println(result.get(Integer.class));
        }

        System.out.println("all --- ");
        results = client.submit("[1,2,3,4,5,6,7,8,9]");
        results.all().get(5000, TimeUnit.MILLISECONDS).stream().map(i -> i.get(Integer.class) * 2).forEach(System.out::println);

        System.out.println("stream them --- ");
        results = client.submit("[1,2,3,4,5,6,7,8,9]");
        results.stream().map(i -> i.get(Integer.class) * 3).forEach(System.out::println);

        cluster.close();
    }

    @Test
    public void shouldReceiveFailureTimeOutOnScriptEval() throws Exception {
        final Cluster cluster = Cluster.create("localhost").build();
        final Client client = cluster.connect();

        try {
            client.submit("Thread.sleep(300);'some-stuff-that-should not return'").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            assertTrue(re.getCause().getMessage().startsWith("Script evaluation exceeded the configured threshold of 200 ms for request"));
        } finally {
            cluster.close();
        }
    }

    @Ignore("not working yet")
    @Test
    public void shouldReceiveFailureTimeOutOnWrite() throws Exception {
        final Cluster cluster = Cluster.create("localhost").build();
        final Client client = cluster.connect();

        try {
            client.submit("Thread.sleep(30000);'some-stuff-that-should not return'").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            assertTrue(re.getCause().getMessage().startsWith("Script evaluation exceeded the configured threshold of 200 ms for request"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldReceiveFailureTimeOutOnTotalSerialization() throws Exception {
        final Cluster cluster = Cluster.create("localhost").build();
        final Client client = cluster.connect();

        try {
            client.submit("(0..<100000)").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            assertTrue(re.getCause().getMessage().endsWith("Serialization of the entire response exceeded the serializeResponseTimeout setting"));
        } finally {
            cluster.close();
        }
    }

    @Ignore("not working yet")
    @Test
    public void shouldReceiveFailureForTimeoutOfIndividualSerialization() throws Exception {
        final Cluster cluster = Cluster.create("localhost").build();
        final Client client = cluster.connect();

        try {
            client.submit("[(0..<100000),'x']").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            assertTrue(re.getCause().getMessage().endsWith("Serialization of an individual result exceeded the serializeResultTimeout setting"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldReceiveFailureOnBadSerialization() throws Exception {
        final Cluster cluster = Cluster.create("localhost").build();
        final Client client = cluster.connect();

        try {
            client.submit("def class C { def C getC(){return this}}; new C()").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            assertTrue(re.getCause().getMessage().equals("Error during serialization: Direct self-reference leading to cycle (through reference chain: java.util.HashMap[\"result\"]->C[\"c\"])"));
        } finally {
            cluster.close();
        }
    }

    @Test
    @Ignore("This test needs to be fixed feedback is retrieved from netty.")
    public void shouldBlockRequestWhenTooBig() throws Exception {
        final Cluster cluster = Cluster.create("localhost").build();
        final Client client = cluster.connect();

        try {
            final String fatty = IntStream.range(0, 65536).mapToObj(String::valueOf).collect(Collectors.joining());
            client.submit("'" + fatty + "'").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            assertTrue(re.getCause().getMessage().equals("Error during serialization: Direct self-reference leading to cycle (through reference chain: java.util.HashMap[\"result\"]->C[\"c\"])"));
        } finally {
            cluster.close();
        }
    }

    @Test
    @Ignore("This test needs to be fixed once we have a real client that can properly deserialize things other than String.")
    public void shouldDeserializeJsonBuilder() throws Exception {
        final Cluster cluster = Cluster.create("localhost").build();
        final Client client = cluster.connect();

        try {
            client.submit("b = new JsonBuilder();b.people{person {fname 'stephen'\nlname 'mallette'}};b").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            assertTrue(re.getCause().getMessage().endsWith("Serialization of an individual result exceeded the serializeResultTimeout setting"));
        } finally {
            cluster.close();
        }
    }
}
