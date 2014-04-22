package com.tinkerpop.gremlin.driver.ser;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ToStringResultSerializerTest {

    private static final MessageSerializer serializer = new ToStringMessageSerializer();
    private static final UUID testId = UUID.fromString("2D62161B-9544-4F39-AF44-62EC49F9A595");
    private static final String testIdString = testId.toString();

    @Test
    public void serializeResponseContainingNull() throws Exception {
        final ResponseMessage msg = ResponseMessage.create(testId, "text/plain").build();
        final String results = serializer.serializeResponseAsString(msg);
        assertEquals(testIdString + ">>200>>text/plain>>null", results);
    }

    @Test
    public void serializeResponseContainingVertex() throws Exception {
        final TinkerGraph g = TinkerFactory.createClassic();
        final Vertex v = (Vertex) g.V().has("name", Compare.EQUAL, "marko").next();
        final ResponseMessage msg = ResponseMessage.create(testId, "text/plain")
                .result(v)
                .build();
        final String results = serializer.serializeResponseAsString(msg);
        assertEquals(testIdString + ">>200>>text/plain>>v[1]", results);
    }

    @Test
    public void deserializeResponse() throws Exception {
        final Optional<ResponseMessage> responseMessage = serializer.deserializeResponse(testIdString + ">>200>>text/plain>>test");
        assertTrue(responseMessage.isPresent());
        assertEquals(testId, responseMessage.get().getRequestId());
        assertEquals(ResultCode.SUCCESS, responseMessage.get().getCode());
        assertEquals("test", responseMessage.get().getResult());
    }
}
