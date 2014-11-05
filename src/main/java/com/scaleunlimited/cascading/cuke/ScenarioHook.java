package com.scaleunlimited.cascading.cuke;

import cucumber.api.Scenario;
import cucumber.api.java.After;
import cucumber.api.java.Before;

public class ScenarioHook {
    @Before
    public void beforeScenario(Scenario scenario) {
        WorkflowContext.setCurrentScenario(scenario);
    }

    @After
    public void afterScenario() {
        WorkflowContext.setCurrentScenario(null);
    }
}
