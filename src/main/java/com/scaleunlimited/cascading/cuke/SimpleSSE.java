package com.scaleunlimited.cascading.cuke;

import java.io.Closeable;
import java.io.IOException;

public class SimpleSSE<V> implements ScenarioStateElement {
	
	private V _value;
	
	public SimpleSSE(V value) {
		_value = value;
	}
	
	public V getValue() {
		return _value;
	}

	@Override
	public void close() throws IOException {
		if (_value instanceof Closeable) {
			((Closeable)_value).close();
		}
	}

}
