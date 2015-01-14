package com.scaleunlimited.cascading.cuke;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("serial")
public class TupleValues extends HashMap<String, String> {

    public TupleValues() {
        super();
    }

    public TupleValues(int initialCapacity) {
        super(initialCapacity);
    }

    public TupleValues(Map<? extends String, ? extends String> m) {
        super(m);
    }

    
}
