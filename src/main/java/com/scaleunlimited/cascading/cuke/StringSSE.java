package com.scaleunlimited.cascading.cuke;

import java.io.IOException;

/**
 * Allows you to store a String in the {@link ScenarioState}
 */
public class StringSSE implements ScenarioStateElement {
	
	private String _value;
	
	public StringSSE(String value) {
		_value = value;
	}
	
	public String toString() {
		return _value;
	}

	@Override
	public void close() throws IOException {
		// nothing to release
	}

}
