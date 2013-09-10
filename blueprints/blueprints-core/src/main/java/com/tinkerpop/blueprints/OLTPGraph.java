package com.tinkerpop.blueprints;

/**
 * An OLTPGraph is a container object for a collection of vertices and a collection edges.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface OLTPGraph {

    public Vertex addVertex(Property... properties);

    public GraphQuery query();

    public void commit();

    public void rollback();

    public Features getFeatures();

}
