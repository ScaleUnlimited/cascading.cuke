package com.scaleunlimited.cascading.cuke;

import java.util.HashMap;
import java.util.Map;

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
        if (!WorkflowInterface.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format("The class %s must implement the WorkflowInterface", workflowName));
        }

        WorkflowContext context = WORKFLOW_CONTEXTS.get(workflowName);
        if (context == null) {
            // set up default context
            context = new WorkflowContext(workflowName, clazz);
            WORKFLOW_CONTEXTS.put(workflowName, context);
        } else if (context.getWorkflowClass().equals(clazz)) {
            // Ignore duplicate registration
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
    private WorkflowParams _params;
    private FlowResult _result;
    private Exception _failure;
    private WorkflowPlatform _platform;
    
    public WorkflowContext(String name, Class clazz) {
    	_name = name;
        _class = clazz;
        _params = new WorkflowParams();
        _platform = WorkflowPlatform.LOCAL;
    }
    
    public WorkflowParams getParamsCopy() {
        return new WorkflowParams(_params);
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
        _params.put(paramName, paramValue);
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
}
