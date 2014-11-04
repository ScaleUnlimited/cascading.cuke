package com.scaleunlimited.cascading.cuke;

import java.util.HashMap;
import java.util.Map;

import cucumber.api.Scenario;
import cucumber.api.java.After;
import cucumber.api.java.Before;

import com.scaleunlimited.cascading.FlowResult;

public class WorkflowContext {
    
    private static Map<String, WorkflowContext> WORKFLOW_CONTEXTS = new HashMap<String, WorkflowContext>();
    private static String CURRENT_WORKFLOW = null;

    public static String getCurrentWorkflowName() {
        String workflowName = CURRENT_WORKFLOW;
        if (workflowName == null) {
            throw new IllegalStateException(String.format("The current workflow context hasn't been set"));
        }
        
        return workflowName;
    }
    
    public static WorkflowContext getCurrentContext() {
        return getContext(getCurrentWorkflowName());
    }
    
    public static WorkflowPlatform getCurrentPlatform() {
    	return getCurrentContext().getDefaultPlatform();
    }
    
	public static WorkflowInterface getCurrentWorkflow() {
		return getCurrentContext().getWorkflow();
	}

    public static WorkflowContext getContext(String workflowName) {
        CURRENT_WORKFLOW = workflowName;

        WorkflowContext context = WORKFLOW_CONTEXTS.get(workflowName);
        if (context == null) {
            throw new IllegalStateException(String.format("Context for workflow %s does not exist", workflowName));
        }
        
        return context;
    }

    public static void registerWorkflow(Class clazz) {
        registerWorkflow(clazz.getCanonicalName(), clazz);
    }
    
    public static void registerWorkflow(String workflowName, Class clazz) {
        WorkflowContext context = WORKFLOW_CONTEXTS.get(workflowName);
        if (context == null) {
            if (!WorkflowInterface.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException(String.format("The class %s must implement the WorkflowInterface", workflowName));
            }

            // set up default context
            context = new WorkflowContext(workflowName, clazz);
            WORKFLOW_CONTEXTS.put(workflowName, context);
        } else if (context.getWorkflowClass().equals(clazz)) {
            // Ignore duplicate registration - just clear out parameters
        	context.resetParameters();
        } else {
            throw new IllegalStateException(String.format(   "The workflow name %s has already been registered with class %s", 
                                                            workflowName, 
                                                            context.getWorkflowClass().getCanonicalName()));
        }

        CURRENT_WORKFLOW = workflowName;
    }
    
	private String _name;
    private Class _class;
    private WorkflowInterface _workflow;

    private String _testPath;
    private WorkflowPlatform _platform;
    private Map<String, WorkflowParams> _params;
    private WorkflowParams _backgroundParams;
    private Scenario _scenario;

    // Results from running the workflow
    private FlowResult _result;
    private Exception _failure;

    public WorkflowContext() {
        _backgroundParams = new WorkflowParams();
        _params = new HashMap<>();
        _platform = WorkflowPlatform.LOCAL;
    }

    public WorkflowContext(String name, Class clazz) {
        this();
    	_name = name;
        _class = clazz;
    }


    @Before
    public void beforeScenario(Scenario scenario) {
        _scenario = scenario;
        _params.put(_scenario.getId(), new WorkflowParams(_backgroundParams));
    }

    @After
    public void afterScenario() {
    }

    public WorkflowParams getParamsCopy() {
        return new WorkflowParams(getParams());
    }
    
	public WorkflowParams getParams() {
		return _scenario == null ? _backgroundParams : _params.get(_scenario.getId());
	}

    public void addResult(FlowResult result) {
        _result = result;
    }
    
    public long getCounter(Enum counter) {
        return _result.getCounterValue(counter);
    }
    
    public long getCounter(String group, String counter) {
        return _result.getCounterValue(group, counter);
    }

    public Map<String, Long> getCounters() {
    	return _result.getCounters();
    }
    
    public void addParameter(String paramName, String paramValue) {
        getParams().put(paramName, paramValue);
    }

    public void resetParameters() {
        final WorkflowParams params = getParams();
        if (params == _backgroundParams) return;
        params.reset();
        params.putAll(_backgroundParams);
	}
    
    public void addFailure(Exception e) {
        _failure = e;
    }

    public Exception getFailure() {
        return _failure;
    }
    
    public Class getWorkflowClass() {
        return _class;
    }
    
    public String getWorkflowName() {
    	return _name;
    }
    
    public WorkflowInterface getWorkflow() {
    	if (_workflow == null) {
            try {
            	_workflow = (WorkflowInterface)_class.newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Unable to create the %s workflow", _name), e);
            }

    	}
    	
    	return _workflow;
    }
    
    public void setDefaultPlatform(WorkflowPlatform platform) {
        _platform = platform;
    }
    
    public WorkflowPlatform getDefaultPlatform() {
        return _platform;
    }

	public void setTestDir(String testPath) {
		_testPath = testPath;
	}

	public String getTestDir() {
		return _testPath;
	}

}
