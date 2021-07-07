package org.apache.tinkerpop.gremlin.language.examples.thrift.mydomain;

import org.apache.tinkerpop.gremlin.language.examples.thrift.ValueEncoding;

public class BoundingBox {
    public static final ValueEncoding BOUNDINGBOX_ENCODING = EncodingUtils.createEncoding(
            "org/apache/tinkerpop/gremlin/language/examples/thrift/mydomain.BoundingBox",
            BoundingBox.class);

    private GeoPoint northwest;
    private GeoPoint southeast;

    public BoundingBox() {}

    public BoundingBox(GeoPoint northwest, GeoPoint southeast) {
        this.northwest = northwest;
        this.southeast = southeast;
    }

    public GeoPoint getNorthwest() {
        return northwest;
    }

    public void setNorthwest(GeoPoint northwest) {
        this.northwest = northwest;
    }

    public GeoPoint getSoutheast() {
        return southeast;
    }

    public void setSoutheast(GeoPoint southeast) {
        this.southeast = southeast;
    }
}
