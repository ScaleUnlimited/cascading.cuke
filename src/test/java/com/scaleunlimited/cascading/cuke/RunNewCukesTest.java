package com.scaleunlimited.cascading.cuke;

import org.junit.runner.RunWith;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;

/**
 * This class only runs scenarios that have the "@new" tag,
 * which is useful when working on new step definitions or
 * scenarios in a feature, as there's not a good way to focus
 * on just part of a feature file.
 *
 */
@RunWith(Cucumber.class)
@CucumberOptions(plugin = {"pretty"}, tags = {"@new"})
public class RunNewCukesTest {

}
