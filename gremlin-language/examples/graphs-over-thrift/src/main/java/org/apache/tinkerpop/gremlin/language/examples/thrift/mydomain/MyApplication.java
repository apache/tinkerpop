package org.apache.tinkerpop.gremlin.language.examples.thrift.mydomain;

import org.apache.tinkerpop.gremlin.language.examples.thrift.ValueEncodingRegistry;

public class MyApplication {
    public static final ValueEncodingRegistry SUPPORTED_ENCODINGS = new ValueEncodingRegistry(
            BoundingBox.BOUNDINGBOX_ENCODING,
            GeoPoint.GEOPOINT_ENCODING);
}
