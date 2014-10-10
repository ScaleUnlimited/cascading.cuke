cascading.cuke is an open-source tool that lets you write [Cascading](http://www.cascading.org) workflow tests using [Gherkin](https://github.com/cucumber/cucumber/wiki/Gherkin), the domain-specific language that's part of the [Cucumber](http://cukes.info/) project.

Cucumber's goal is to support "Business-driven Development" (BDD), where teams can use something akin to plain English to describe how the systems should behave, based on a context, some actions, and the expected results. To support this goal, Cucumber makes heavy use of the Ruby language and its support for dynamic code.

Cascading is based on Java, so we've created a much simpler environment that's optimized for specifically testing Cascading workflows. As such, we have a number of pre-defined "step definitions" that support creation of input data, the execution of Cascading workflows, and the validation of results.

In order to take full advantage of cascading.cuke, the workflow class being tested will need to implement the WorkflowInterface to make things like input data creation more generic.

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
		Given the com.scaleunlimited.cascading.cuke package contains the WordCountTool workflow
		And the workflow will be run locally
		And these parameters for the workflow:
			| input | build/test/WordCountTool/input |
			| output | build/test/WordCountTool/output |
	
	Scenario: Fail if there is no input directory
		Given the workflow "build/test/WordCountTool/input" directory has been deleted
		When the workflow is run
		Then the workflow should fail
	
	Scenario: Words should be lower-cased before being counted
		Given text records in the workflow "build/test/WordCountTool/input" directory:
			| Test TEST test |
		When the workflow is run 
		Then the workflow "output" result should have a record where:
			| word | test |
			| count | 3 |

	Scenario: Words should be counted across text lines
		Given text records in the workflow "build/test/WordCountTool/input" directory:
			| The quick red fox jumped over the lazy brown dog. |
			| brown cow |
		When the workflow is run 
		Then the workflow "output" result should have a record where:
			| word | brown |
			| count | 2 |

	Scenario: Words should not include punctuation
		Given text records in the workflow "build/test/WordCountTool/input" directory:
			| My cat. |
		When the workflow is run
		Then the workflow "output" result should have a record where:
			| word | cat |
			| count | 1 |

	Scenario: We should be able to filter out low-frequency words
		Given text records in the workflow "build/test/WordCountTool/input" directory:
			| one two three |
			| word1 word1 |
			| word2 word2 |
		And the workflow parameter mincount is 2
		When the workflow is run
		Then the workflow "output" result should have records where:
			| word | count |
			| word1 | 2 |
		And the workflow "output" result should only have records where:
			| word | count |
			| word1 | 2 |
			| word2 | 2 |
		And the workflow "output" result should not have records where:
			| word |
			| one |
			| two |
			| three |

	Scenario: We should keep track of the number of input records, using
			Hadoop counters.
		Given text records in the workflow "build/test/WordCountTool/input" directory:
			| line one |
			| line two |
			| line three |
		When the workflow is run
		Then the workflow "VALID_LINES" counter should be 3
		And the workflow "INVALID_LINES" counter should be 0
		And the workflow "WORDS" counter should be more than 5
		And the workflow "WORDS" counter should be greater than 5
		And the workflow "WORDS" counter should be > 5
		And the workflow "WORDS" counter should be at least 6
		And the workflow "WORDS" counter should be >= 6
		And the workflow "WordCountCounters.WORDS" counter should be < 7
		And the workflow "WORDS" counter should be at most 6
		And the workflow "WORDS" counter should be <= 6
        	
</pre>
