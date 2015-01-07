package com.tinkerpop.gremlin.process.util;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 *
 * @author Bob Briody (http://bobbriody.com)
 */
public class MetricsUtil implements Metrics, Serializable, Cloneable {
    static final TimeUnit SOURCE_UNIT = TimeUnit.NANOSECONDS;

    // Note: if you add new members then you probably need to add them to the copy constructor;

    private String id;
    private String name;

    private long count;
    private long durationNs = 0l;
    private double percentDuration = -1;
    private long tempTime = -1l;

    private Map<String, String> annotations = new HashMap<>();
    private Map<String, MetricsUtil> children = new HashMap<>();

    private MetricsUtil(){
       // necessary for kryo serialization
    }

    public MetricsUtil(final String id, final String name) {
        this.id = id;
        this.name = name;
    }

    private MetricsUtil(final MetricsUtil metrics) {
        this.id = metrics.id;
        this.name = metrics.name;
        this.count = metrics.count;
        this.durationNs = metrics.durationNs;
        this.percentDuration = metrics.percentDuration;
        metrics.children.values().forEach(child -> children.put(child.id, child.clone()));
    }

    public void addChild(MetricsUtil metricsUtil) {
        this.children.put(metricsUtil.getId(), metricsUtil);
    }

    public void start() {
        if (-1 != this.tempTime) {
            throw new IllegalStateException("Concurrent MetricsUtil start. Stop timer before starting timer.");
        }
        this.tempTime = System.nanoTime();
    }

    public void stop() {
        if (-1 == this.tempTime)
            throw new IllegalStateException("MetricsUtil has not been started. Start timer before starting timer");
        this.durationNs = this.durationNs + (System.nanoTime() - this.tempTime);
        this.tempTime = -1;
    }

    @Override
    public long getDuration(TimeUnit unit) {
        return unit.convert(this.durationNs, SOURCE_UNIT);
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    public void finish(final long count) {
        stop();
        this.count += count;
    }

    public void incrementCount(final long count) {
        this.count += count;
    }

    public void aggregate(MetricsUtil other) {
        this.durationNs += other.durationNs;
        this.count += other.count;

        // Merge annotations. If a duplicate is found then append it to a comma-separated list.
        for (Map.Entry<String, String> p : other.annotations.entrySet()) {
            if (this.annotations.containsKey(p.getKey())) {
                String existing = this.annotations.get(p.getKey());
                if (!existing.equals(p.getValue())) {
                    this.annotations.put(p.getKey(), existing + "," + p.getValue());
                }
            } else {
                this.annotations.put(p.getKey(), p.getValue());
            }
        }

        this.annotations.putAll(other.annotations);
        other.children.values().forEach(child -> {
            MetricsUtil thisChild = this.children.get(child.getId());
            if (thisChild == null) {
                thisChild = new MetricsUtil(child.getId(), child.getName());
                this.children.put(thisChild.getId(), thisChild);
            }
            thisChild.aggregate(child);
        });
    }

    @Override
    public double getPercentDuration() {
        return this.percentDuration;
    }

    @Override
    public Collection<MetricsUtil> getChildren() {
        return children.values();
    }

    @Override
    public MetricsUtil getChild(String metricsId) {
        return children.get(metricsId);
    }

    @Override
    public Map<String, String> getAnnotations() {
        return annotations;
    }

    /**
     * Set an annotation value. Duplicates will be overwritten.
     * @param key
     * @param value
     */
    public void setAnnotation(String key, String value) {
        annotations.put(key, value);
    }

    public void setPercentDuration(final double percentDuration) {
        this.percentDuration = percentDuration;
    }

    @Override
    public MetricsUtil clone() {
        MetricsUtil clone = new MetricsUtil(this);
        return clone;
    }

    @Override
    public String toString() {
        return "MetricsUtil{" +
                "durationNs=" + durationNs +
                ", count=" + count +
                ", name='" + name + '\'' +
                ", id='" + id + '\'' +
                '}';
    }

    /*package*/ void setDuration(final long duration) {
        this.durationNs = duration;
    }
}
