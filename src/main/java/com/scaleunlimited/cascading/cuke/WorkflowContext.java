package com.scaleunlimited.cascading.cuke;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import com.scaleunlimited.cascading.FlowCounters;

import cucumber.api.Scenario;

public class WorkflowContext {
    
    private static Map<String, WorkflowContext> WORKFLOW_CONTEXTS = new HashMap<String, WorkflowContext>();
    private static String CURRENT_WORKFLOW = null;
    private static Scenario CURRENT_SCENARIO = null;

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

    public static void registerWorkflow(Class<?> clazz) {
        registerWorkflow(clazz.getCanonicalName(), clazz);
    }
    
    public static void registerWorkflow(String workflowName, Class<?> clazz) {
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
        	context.resetParameters(null);
        } else {
            throw new IllegalStateException(String.format(   "The workflow name %s has already been registered with class %s", 
                                                            workflowName, 
                                                            context.getWorkflowClass().getCanonicalName()));
        }

        CURRENT_WORKFLOW = workflowName;
    }

    public static void setCurrentScenario(Scenario scenario) {
        CURRENT_SCENARIO = scenario;
        if (CURRENT_WORKFLOW == null) {
            return;
        }
        
        final WorkflowContext currentContext = getCurrentContext();
        if (!currentContext._params.containsKey(CURRENT_SCENARIO)) {
            currentContext._params.put(CURRENT_SCENARIO, new WorkflowParams());
        }
    }
    
	private String _name;
    private Class<?> _class;
    private WorkflowInterface _workflow;

    private String _testPath;
    private WorkflowPlatform _platform;
    private Map<Scenario, WorkflowParams> _params;
//    private WorkflowParams _backgroundParams;

    // Results from running the workflow or tool
    private Map<String, Long> _counters;
    private Exception _failure;

    public WorkflowContext(String name, Class<?> clazz) {
        _params = new HashMap<Scenario, WorkflowParams>();
        _platform = WorkflowPlatform.LOCAL;
    	_name = name;
        _class = clazz;
    }

    public WorkflowParams getParamsCopy() {
        return new WorkflowParams(getParams());
    }
    
	public synchronized WorkflowParams getParams() {
        WorkflowParams params = _params.get(CURRENT_SCENARIO);
        if (params == null) {
            params = new WorkflowParams();
            _params.put(CURRENT_SCENARIO, params);
        }
        return params;
	}

    public void addResult(Map<String, Long> result) {
        _counters = result;
    }
    
    @SuppressWarnings("rawtypes")
    public long getCounter(Enum counter) {
        Long result = _counters.get(FlowCounters.getCounterKey(counter));
        return (result == null) ? 0 : result;
    }
    
    public long getCounter(String group, String counter) {
        Long result = _counters.get(FlowCounters.getCounterKey(group, counter));
        return (result == null) ? 0 : result;
    }

    public Map<String, Long> getCounters() {
    	return _counters;
    }
    
    public void addParameter(String paramName, String paramValue) {
        getParams().put(paramName, paramValue);
    }

    public void resetParameters(WorkflowParams reset) {
        if (reset != null) {
            _params.put(CURRENT_SCENARIO, reset);
        } else {
            final WorkflowParams params = getParams();
            params.reset();
        }
	}
    
    public void addFailure(Exception e) {
        _failure = e;
    }

    public Exception getFailure() {
        return _failure;
    }
    
    public Class<?> getWorkflowClass() {
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
	    // Fix up Unix/Mac-style path names for Windows, then do macro substitution
	    // for ${scenario} with the scenario name.
	    testPath = testPath.replaceAll("/", Matcher.quoteReplacement(File.separator));
		_testPath = testPath.replaceAll("\\$\\{scenario\\}", getScenarioName());
	}

	private String getScenarioName() {
	    // For testing purposes, we don't have a current scenario
		return makePathSafe(CURRENT_SCENARIO == null ? "scenario" : CURRENT_SCENARIO.getName());
	}

    /*
     * For a list of characters that are invalid in paths, see:
     * 
     * http://msdn.microsoft.com/en-us/library/windows/desktop/aa365247%28v=vs.85%29.asp
    
    < (less than)
    > (greater than)
    : (colon)
    " (double quote)
    / (forward slash)
    \ (backslash)
    | (vertical bar or pipe)
    ? (question mark)
    * (asterisk)

     */
	protected static String makePathSafe(String name) {
	    name = name.trim();
        name = name.replaceAll("[\u0001-\u001f<>:\"/\\\\|?*\u007f]+", "");
        name = name.replaceAll("[ ]+", "_");
        if (name.length() > 255) {
            name = name.substring(0, 255);
        }
        
        return name;
	}
	
	public String getTestDir() throws IOException {
	    if (getDefaultPlatform() == WorkflowPlatform.LOCAL) {
	        return new File(_testPath).getCanonicalPath();
	    } else {
	        return _testPath;
	    }
	}

}
