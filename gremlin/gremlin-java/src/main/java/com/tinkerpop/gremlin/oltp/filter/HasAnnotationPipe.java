package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.query.util.HasContainer;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasAnnotationPipe extends FilterPipe<Element> {

    public HasContainer hasContainer;
    public String propertyKey;

    public HasAnnotationPipe(final Pipeline pipeline, final String propertyKey, final HasContainer hasContainer) {
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
