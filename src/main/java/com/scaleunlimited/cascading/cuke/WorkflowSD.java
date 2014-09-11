package com.scaleunlimited.cascading.cuke;

import cascading.flow.Flow;

import com.scaleunlimited.cascading.FlowResult;
import com.scaleunlimited.cascading.FlowRunner;

@SuppressWarnings("rawtypes")
public abstract class WorkflowSD extends BaseStepDefinition {

	public abstract Flow createFlow();
    
    String _name;

	public WorkflowSD(String name) {
		super();
		_name = name;
		setRegex(String.format("the %s workflow is run", getName()));
	}

	@Override
	public void run() throws StepFailureException {
		super.run();
		FlowResult flowResult;
		try {
			flowResult = FlowRunner.run(createFlow());
		} catch (Exception e) {
			throw new StepFailureException("Flow failed", e);
		}
		
		_scenarioState.put(_name, new FlowResultSSE(flowResult));
	}
	
	protected String getName() {
		return _name;
	}
}
