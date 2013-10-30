package com.tinkerpop.blueprints.io;

import com.tinkerpop.blueprints.Element;

import java.util.Comparator;

/**
 * Elements are sorted in lexicographical order of IDs.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class LexicographicalElementComparator implements Comparator<Element> {
    @Override
    public int compare(final Element a, final Element b) {
        return a.getId().toString().compareTo(b.getId().toString());
    }
}
