package com.scaleunlimited.cascading.cuke;

import org.junit.runner.RunWith;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;

/**
 * This class only runs scenarios that DO NOT the "@new" tag,
 * since the RunNewCukesTest takes care of those for us.
 *
 */
@RunWith(Cucumber.class)
@CucumberOptions(plugin = {"pretty"}, tags = {"~@new"})
public class RunCukesTest {

}
