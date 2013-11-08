package com.tinkerpop.blueprints.mailbox;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphMemory {

    public <R> R get(final String key);

    public void setIfAbsent(String key, Object value);

    public long increment(String key, long delta);

    public long decrement(String key, long delta);

    public int getIteration();

    public long getRuntime();

    public boolean isInitialIteration();

    //public <M extends Serializable> Mailbox<M> getMailbox();

    /*public void min(String key, double value);

    public void max(String key, double value);

    public void avg(String key, double value);

    public <V> void update(String key, Function<V, V> update, V defaultValue);*/

    // public Map<String, Object> getCurrentState();
}
