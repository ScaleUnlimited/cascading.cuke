package com.scaleunlimited.cascading.cuke;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WorkflowParams {

    private Map<String, String> _params;
    
    public WorkflowParams() {
        _params = new HashMap<String, String>();
    }
    
    public WorkflowParams(WorkflowParams source) {
        _params = new HashMap<String, String>(source._params);
    }
    
    public void put(String paramName, String paramValue) {
        _params.put(paramName, paramValue);
    }

    public void putAll(WorkflowParams source) {
        _params.putAll(source._params);
    }
    
    public String get(String paramName) {
        String paramValue = _params.get(paramName);
        if (paramValue == null) {
            throw new IllegalArgumentException(String.format("Parameter %s has no value", paramName));
        } else {
            return paramValue;
        }
    }
    
    public String getOptional(String paramName) {
        return _params.get(paramName);
    }
    
    public String remove(String paramName) {
        String paramValue = _params.remove(paramName);
        if (paramValue == null) {
            throw new IllegalArgumentException(String.format("Parameter %s has no value", paramName));
        } else {
            return paramValue;
        }
    }
    
    public String removeOptional(String paramName) {
        return _params.remove(paramName);
    }

    public boolean isEmpty() {
        return _params.isEmpty();
    }
    
    public Set<String> getNames() {
        return _params.keySet();
    }
    
    @Override
    public String toString() {
        return _params.toString();
    }

	protected void reset() {
		_params.clear();
	}

}
