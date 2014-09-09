package com.scaleunlimited.cascading.cuke;

import java.io.IOException;

public class LongSSE implements ScenarioStateElement {
	
	private long _value;
	
	public LongSSE(long value) {
		_value = value;
	}
	
	public long toLong() {
		return _value;
	}

	@Override
	public void close() throws IOException {
		// nothing to release
	}

}
