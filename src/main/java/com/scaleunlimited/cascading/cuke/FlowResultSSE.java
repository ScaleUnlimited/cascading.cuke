package com.scaleunlimited.cascading.cuke;

import java.io.IOException;

import com.scaleunlimited.cascading.FlowResult;

public class FlowResultSSE implements ScenarioStateElement {
	
	private FlowResult _value;
	
	public FlowResultSSE(FlowResult value) {
		_value = value;
	}
	
	public FlowResult toFlowResult() {
		return _value;
	}

	@Override
	public void close() throws IOException {
		// nothing to release
	}

}
