package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.MultiIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchPipe<S, E> extends AbstractPipe<S, E> {

    private Iterator<Holder<E>> iterator = Collections.emptyIterator();
    private final Pipeline[] pipelines;
    private final String inAs;
    private final String outAs;

    public MatchPipe(final String inAs, final String outAs, final Pipeline pipeline, final Pipeline... pipelines) {
        super(pipeline);
        this.inAs = inAs;
        this.outAs = outAs;
        this.pipelines = pipelines;
        for (int i = 0; i < this.pipelines.length; i++) {
            for (int j = i + 1; j < this.pipelines.length; j++) {
                for (final String key : (Set<String>) pipelines[i].getAs()) {
                    if (pipelines[j].getAs().contains(key)) {
                        pipelines[j].getAs(key).setStarts(pipelines[i].getAs(key));
                    }
                }
            }
        }
    }

    public Holder<E> processNextStart() {
        while (true) {
            if (this.iterator.hasNext())
                return this.iterator.next();
            else {
                final Holder<S> start = this.starts.next();
                this.getAs(this.inAs).forEach(pipe -> pipe.addStart(start.makeSibling(start.get())));
                this.iterator = new MultiIterator(this.getAs(outAs));
            }
        }
    }

    public List<Pipe> getAs(final String key) {
        return Stream.of(this.pipelines)
                .filter(p -> p.getAs().contains(key))
                .map(p -> p.getAs(key))
                .collect(Collectors.<Pipe>toList());
    }

}
