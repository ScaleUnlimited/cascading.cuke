package com.scaleunlimited.cascading.cuke;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A shared state where {@link StepDefinition} objects can communicate with
 * one another. For example, the parameters for a work flow can be placed into
 * here by separate And steps (e.g., "And targetDate parameter is 2014-08-02").
 * When the work flow StepDefinition gets run, it can then load the parameter
 * values from here.
 */
public class ScenarioState {
	Map<String, ScenarioStateElement> _elementMap;
	
	public ScenarioState() {
		_elementMap = new HashMap<String, ScenarioStateElement>();
	}
	
	// TODO Delegate the other Map methods?
	
	public boolean containsKey(String key) {
		return _elementMap.containsKey(key);
	}
	
	public ScenarioStateElement get(String key) {
		return _elementMap.get(key);
	}
	
	public ScenarioStateElement put(String key, ScenarioStateElement element) {
		return _elementMap.put(key, element);
	}
	
	public Collection<ScenarioStateElement> values() {
		return _elementMap.values();
	}
	
	public Set<Map.Entry<String, ScenarioStateElement>> entrySet() {
		return _elementMap.entrySet();
	}
	
	public void clear() throws IOException {
		for (ScenarioStateElement element : _elementMap.values()) {
			element.close();
		}
		_elementMap.clear();
	}
}
