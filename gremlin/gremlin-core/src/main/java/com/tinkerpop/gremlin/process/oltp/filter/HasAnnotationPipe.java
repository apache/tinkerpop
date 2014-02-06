package com.tinkerpop.gremlin.process.oltp.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.query.util.HasContainer;
import com.tinkerpop.gremlin.process.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasAnnotationPipe extends FilterPipe<Element> {

    public HasContainer hasContainer;
    public String propertyKey;

    public HasAnnotationPipe(final Traversal pipeline, final String propertyKey, final HasContainer hasContainer) {
        super(pipeline);
        this.propertyKey = propertyKey;
        this.hasContainer = hasContainer;
        this.setPredicate(holder -> {
            final Property<AnnotatedList> property = holder.get().getProperty(this.propertyKey);
            return property.isPresent() &&
                    property.get().query()
                            .has(this.hasContainer.key, this.hasContainer.predicate, this.hasContainer.value)
                            .annotatedValues().iterator().hasNext();
        });
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.propertyKey, this.hasContainer);
    }
}
