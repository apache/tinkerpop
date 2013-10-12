package com.tinkerpop.gremlin.server;

import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OpProcessor {
    private static Optional<OpProcessor> singleton = Optional.empty();

    private OpProcessor() { }

    public Optional<Op> select(final RequestMessage message) {
        final Optional<Op> op;
        switch (message.op) {
            case "eval":
                op = Optional.of((Op) new Op.EvalOp());
                break;
            default:
                op  = Optional.empty();
                break;
        }

        return op;
    }

    public static OpProcessor instance() {
        if (!singleton.isPresent())
            singleton = Optional.of(new OpProcessor());
        return singleton.get();
    }
}
