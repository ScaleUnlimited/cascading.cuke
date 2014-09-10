cascading.cuke is an open-source tool that lets you write [Cascading](http://www.cascading.org) workflow tests using [Gherkin](https://github.com/cucumber/cucumber/wiki/Gherkin), the domain-specific language that's part of the [Cucumber](http://cukes.info/) project.

Cucumber's goal is to support "Business-driven Development" (BDD), where teams can use something akin to plain English to describe how the systems should behave, based on a context, some actions, and the expected results. To support this goal, Cucumber makes heavy use of the Ruby language and its support for dynamic code.

Cascading is based on Java, so we've created a much simpler environment that's optimized for specifically testing Cascading workflows. As such, we have a number of pre-defined "step definitions" that support creation of input data, the execution of Cascading workflows, and the validation of results.

In order to take full advantage of cascading.cuke, some of your classes will need to implement extra interfaces that are used to make things like input data creation more generic.

# Running the tool

Assuming you've downloaded a distribution tarball and unpacked it, then from the command line (in the distribution directory) you would execute the run-cuke tool. For example:

`% bin/run-cuke -f examples/mytests.cuke -cp examples/myworkflow.jar`

This will execute the test steps contained in the `mytests.cuke` file, using the Cascading workflow code contained in the `myworkflow.jar` jar file. The results are printed to the terminal, using the standard Cucumber color scheme of green for any passing tests.

# The Gherkin syntax

For a good list of Gherkin features, check out the [Behat documentation](http://docs.behat.org/en/latest/guides/1.gherkin.html).

# An example file

Here are the contents of the mytests.cuke file referenced above:

<pre>Feature: Count words
	In order to determine statistics for the English language,
	we need to count the occurrences of each individual word,
	using input data from multiple text files.
	
	Background:
		Given text records in an "input" directory
			| Test TEST test |
			| The quick red fox jumped over the lazy brown dog. |
			| brown cow |
		And the input parameter for the WordCountTool tool is "input"
		And the output parameter for the WordCountTool tool is "output"
		
	Scenario: Words should be lowercased before being counted
		Then the results in "output" should have a record where "word" is "test" and "count" is 3

	Scenario: Words should be counted across text lines
		Then the results in "output" should have a record where "word" is "brown" and "count" is 2

	Scenario: Words should not include punctuation
		Then the results in "output" should have a record where "word" is "dog" and "count" is 1
</pre>
