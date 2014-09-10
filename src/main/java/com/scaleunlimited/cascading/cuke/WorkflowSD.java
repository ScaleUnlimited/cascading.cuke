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
		
		// TODO Here we're checking the assertions within WorkflowSD.run(),
		// instead of performing these checks within each
		// WorkflowCounterAssertionSD.run().  This is because the latter
		// don't get instantiated with each matching step's details, but rather
		// stuff those details into the ScenarioState element each creates.
		// Note that this is similar to the way that a WorkflowParameterSD
		// works (i.e., its run() method does nothing).
		if (!WorkflowCounterAssertionSSE.checkScenarioAssertions(	flowResult.getCounters(),
																	_scenarioState)) {
			throw new StepFailureException("At least one counter assertion failed");
		}
	}
	
	protected String getName() {
		return _name;
	}
}
