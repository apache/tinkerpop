package com.tinkerpop.gremlin.process.computer.ranking.pagerank;

import com.tinkerpop.gremlin.process.computer.MessageCombiner;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankMessageCombiner implements MessageCombiner<Double> {

    private static final Optional<PageRankMessageCombiner> INSTANCE = Optional.of(new PageRankMessageCombiner());

    private PageRankMessageCombiner() {

    }

    @Override
    public Double combine(final Double messageA, final Double messageB) {
        return messageA + messageB;
    }

    public static Optional<PageRankMessageCombiner> instance() {
        return INSTANCE;
    }
}
