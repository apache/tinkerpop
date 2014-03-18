package com.tinkerpop.gremlin.server.op;

import com.tinkerpop.gremlin.server.OpProcessor;
import com.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Uses {@code ServiceLoader} to load {@link OpProcessor} instances into a cache.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class OpLoader {
    private static final Logger logger = LoggerFactory.getLogger(OpLoader.class);

    private static final Map<String, OpProcessor> processors = new HashMap<>();

    static {
        ServiceLoader.load(OpProcessor.class).forEach(op -> {
            final String name = op.getName();
            logger.info("Adding the {} OpProcessor.", name.equals(StandardOpProcessor.OP_PROCESSOR_NAME) ? "standard" : name);
            if (processors.containsKey(name))
                throw new RuntimeException(String.format("There is a naming conflict with the %s OpProcessor implementations.", name));

            processors.put(name, op);
        });
    }

    public static Optional<OpProcessor> getProcessor(final String name) {
        return Optional.ofNullable(processors.get(name));
    }
}
