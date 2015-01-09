package com.tinkerpop.gremlin.structure.io.kryo;

import org.junit.Test;

import static org.junit.Assert.assertNotSame;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class KryoMapperTest {
    @Test
    public void shouldGetMostRecentVersion() {
        final KryoMapper.Builder b = KryoMapper.build();
        assertNotSame(b, KryoMapper.build());
    }
}
