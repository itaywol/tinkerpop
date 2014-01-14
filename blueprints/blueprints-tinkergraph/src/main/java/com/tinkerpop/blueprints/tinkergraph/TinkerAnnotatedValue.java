package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.Annotations;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerAnnotatedValue<V> implements AnnotatedValue<V> {

    private final V value;
    private final Annotations annotations;

    public TinkerAnnotatedValue(final V value, final Annotations annotations) {
        this.value = value;
        this.annotations = new TinkerAnnotations();
        annotations.getKeys().forEach(k -> this.annotations.set(k, annotations.get(k).get()));
    }

    public V getValue() {
        return this.value;
    }

    public Annotations getAnnotations() {
        return this.annotations;
    }

    public void remove() {

    }

    public String toString() {
        return StringFactory.annotatedValueString(this);
    }

}