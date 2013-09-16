package com.tinkerpop.blueprints.util;

/**
 * The ExceptionFactory provides standard exceptions that should be used by all Blueprints implementations.
 * This ensures that the look-and-feel of all implementations are the same in terms of terminology and punctuation.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExceptionFactory {

    // Graph related exceptions

    public static IllegalArgumentException vertexIdCanNotBeNull() {
        return new IllegalArgumentException("Vertex id can not be null");
    }

    public static IllegalArgumentException edgeIdCanNotBeNull() {
        return new IllegalArgumentException("Edge id can not be null");
    }

    public static IllegalArgumentException vertexWithIdAlreadyExists(final Object id) {
        return new IllegalArgumentException("Vertex with id already exists: " + id);
    }

    public static IllegalArgumentException edgeWithIdAlreadyExist(final Object id) {
        return new IllegalArgumentException("Edge with id already exists: " + id);
    }

    public static IllegalArgumentException bothIsNotSupported() {
        return new IllegalArgumentException("A direction of BOTH is not supported");
    }

    public static IllegalStateException vertexWithIdDoesNotExist(final Object id) {
        return new IllegalStateException("Vertex with id does not exist: " + id);
    }

    // Element related exceptions

    public static IllegalArgumentException propertyKeyIsReserved(final String key) {
        return new IllegalArgumentException("Property key is reserved for all elements: " + key);
    }

    public static IllegalArgumentException propertyKeyIdIsReserved() {
        return new IllegalArgumentException("Property key is reserved for all elements: id");
    }

    public static IllegalArgumentException propertyKeyLabelIsReservedForEdges() {
        return new IllegalArgumentException("Property key is reserved for all edges: label");
    }

    public static IllegalArgumentException propertyKeyCanNotBeEmpty() {
        return new IllegalArgumentException("Property key can not be the empty string");
    }

    public static IllegalArgumentException propertyKeyCanNotBeNull() {
        return new IllegalArgumentException("Property key can not be null");
    }

    public static IllegalArgumentException propertyValueCanNotBeNull() {
        return new IllegalArgumentException("Property value can not be null");
    }

    public static IllegalArgumentException edgeLabelCanNotBeNull() {
        return new IllegalArgumentException("Edge label can not be null");
    }

    // IndexableGraph related exceptions

    public static IllegalArgumentException indexAlreadyExists(final String indexName) {
        return new IllegalArgumentException("Index already exists: " + indexName);
    }

    public static IllegalStateException indexDoesNotSupportClass(final String indexName, final Class clazz) {
        return new IllegalStateException(indexName + " does not support class: " + clazz);
    }

    // KeyIndexableGraph related exceptions

    public static IllegalArgumentException classIsNotIndexable(final Class clazz) {
        return new IllegalArgumentException("Class is not indexable: " + clazz);
    }

    public static IllegalArgumentException classForElementCannotBeNull() {
        return new IllegalArgumentException("elementClass argument cannot be null.");
    }

    // TransactionalGraph related exceptions

    public static IllegalStateException transactionAlreadyStarted() {
        return new IllegalStateException("Stop the current transaction before starting another");
    }
}
