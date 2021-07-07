package org.apache.tinkerpop.gremlin.language.examples.thrift.mydomain;

import org.apache.tinkerpop.gremlin.language.examples.thrift.ValueEncoding;

public class GeoPoint {
    public static final ValueEncoding GEOPOINT_ENCODING = EncodingUtils.createEncoding(
            "org/apache/tinkerpop/gremlin/language/examples/thrift/mydomain.GeoPoint",
            GeoPoint.class);

    private float latitudeDegrees;
    private float longitudeDegrees;
    private Float altitudeMeters;

    public GeoPoint() {}

    public GeoPoint(float latitudeDegrees, float longitudeDegrees, Float altitudeMeters) {
        this.latitudeDegrees = latitudeDegrees;
        this.longitudeDegrees = longitudeDegrees;
        this.altitudeMeters = altitudeMeters;
    }

    public float getLatitudeDegrees() {
        return latitudeDegrees;
    }

    public void setLatitudeDegrees(float latitudeDegrees) {
        this.latitudeDegrees = latitudeDegrees;
    }

    public float getLongitudeDegrees() {
        return longitudeDegrees;
    }

    public void setLongitudeDegrees(float longitudeDegrees) {
        this.longitudeDegrees = longitudeDegrees;
    }

    public Float getAltitudeMeters() {
        return altitudeMeters;
    }

    public void setAltitudeMeters(Float altitudeMeters) {
        this.altitudeMeters = altitudeMeters;
    }
}
