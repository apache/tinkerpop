package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.gremlin.MapPipe;
import com.tinkerpop.gremlin.Path;
import com.tinkerpop.gremlin.Pipeline;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectPipe extends MapPipe<Object, List> {

    public SelectPipe(final Pipeline pipeline, final String... ases) {
        super(pipeline, holder -> {
            final Path path = holder.getPath();
            return ases.length == 0 ?
                    path.getAsSteps().stream().map(path::get).collect(Collectors.toList()) :
                    Stream.of(ases).map(path::get).collect(Collectors.toList());
        });
    }
}
