cascading.cuke is an open-source tool that lets you write [Cascading](http://www.cascading.org) workflow tests using [Gherkin](https://github.com/cucumber/cucumber/wiki/Gherkin), the domain-specific language that's part of the [Cucumber](http://cukes.info/) project.

Cucumber's goal is to support "Business-driven Development" (BDD), where teams can use something akin to plain English to describe how the systems should behave, based on a context, some actions, and the expected results. To support this goal, Cucumber makes heavy use of the Ruby language and its support for dynamic code.

Cascading is based on Java, so we've created a much simpler environment that's optimized for specifically testing Cascading workflows. As such, we have a number of pre-defined "step definitions" that support creation of input data, the execution of Cascading workflows, and the validation of results.

In order to take full advantage of cascading.cuke, the workflow class being tested will need to implement the WorkflowInterface to make things like input data creation more generic.

# Running the tool

Assuming you've downloaded a distribution tarball and unpacked it, then from the command line (in the distribution directory) you would execute the run-cuke tool. For example:

`% bin/run-cuke -f examples/word_count.feature -cp examples/wordcount.jar`

This will execute the test steps contained in the `word_count.feature` file, using the Cascading workflow code contained in the `wordcount.jar` jar file. The results are printed to the terminal, using the standard Cucumber color scheme of green for any passing tests.

# The Gherkin syntax

For a good list of Gherkin features, check out the [Behat documentation](http://docs.behat.org/en/latest/guides/1.gherkin.html).

# An example file

Here are the contents of the word_count.features file referenced above:

<pre>Feature: Count words
    In order to determine statistics for the English language,
    we need to count the occurrences of each individual word.

	Background:
		# We always need to associate the workflow class (e.g. WordCountTool) with its package location
		Given the com.scaleunlimited.cascading.cuke package contains the WordCountTool workflow
		
		# We can specify that the workflow will be run locally or "on a cluster". We have to also
		# specify the location for where input & output data will be located (the "test" directory).
		And the workflow will be run locally with test directory "build/test/WordCountTool/"
		
		# This is a table of | <parameter name> | <parameter value> |
		# The ${testdir} macro will be expanded to whatever you set for the test directory previously.
		And these parameters for the workflow:
			| input | ${testdir}/input |
			| output | ${testdir}/output |
	
	Scenario: Fail if there is no input directory
		# This is a special version of running the workflow, where failures are captured vs. killing the step.
		When the workflow run is attempted
		Then the workflow should fail
	
	Scenario: Words should be lower-cased before being counted
		# The directory name here is converted into the actual location by the code under
		# test, where "input" means whatever you set up for the "input" parameter to the workflow.
        Given these text records in the workflow "input" directory:
            | Test TEST test |
            
		When the workflow is run 
		
		# By saying "should have a record" we're implicitly talking about a binary output file, where there
		# are going to be at least one record with all of the | <field name> | <field value> | data elements
		# listed.
        Then the workflow "output" directory should have a record where:
        	| word | test |
			| count | 3 |

	Scenario: Words should be counted across text lines
        Given these text records in the workflow "input" directory:
            | The quick red fox jumped over the lazy brown dog. |
            | brown cow |
		When the workflow is run 
        Then the workflow "output" result should have a record where:
        	| word | brown |
			| count | 2 |

	Scenario: Words should not include punctuation
		Given these text records in the workflow "input" directory:
            | My cat. |
		When the workflow is run
        Then the workflow "output" result should have a record where:
        	| word | cat |
			| count | 1 |

	Scenario: We should be able to filter out low-frequency words
        Given these text records in the workflow "input" directory:
            | one two three |
            | word1 word1 |
            | word2 word2 |
            
        # You can add additional parameters beyond what is set up in the background.
        # You can also change the value of an existing parameter.
        And the workflow parameter "mincount" is "2"
        When the workflow is run

       	# You can test for exact results. Note the format here is to have a header line,
       	# with a list of field names, and then one row per record.
        And the workflow "output" result should only have records where:
        	| word | count |
        	| word1 | 2 |
        	| word2 | 2 |
        # You can also test for exclusion
        And the workflow "output" result should not have records where:
        	| word |
        	| one |
        	| two |
        	| three |
        And the workflow "output" result should not have records where:
        	| count |
        	| 1 |

	Scenario: We should keep track of the number of input records, using
			Hadoop counters.
        Given these text records in the workflow "input" directory:
            | line one |
            | line two |
            | line three |
        When the workflow is run
        
        # The table here has a list of | <counter name> | <counter value> pairs.
		Then the workflow result should have counters where:
			| VALID_LINES | 3 |
			| INVALID_LINES | 0 |
			| WORDS | 6 |
		
		# You can also test individual counters for being in a range
        And the workflow "WORDS" counter should be more than 5
        And the workflow "WORDS" counter should be greater than 5
        And the workflow "WORDS" counter should be > 5
        And the workflow "WORDS" counter should be at least 6
        And the workflow "WORDS" counter should be >= 6
        
        # We can handle fully-qualified counters
        And the workflow "WordCountCounters.WORDS" counter should be < 7
        
        # We can handle long values (greater than 2 billion+), e.g. 10B.
        And the workflow "WORDS" counter should be less than 10000000000
        And the workflow "WORDS" counter should be at most 6
        And the workflow "WORDS" counter should be <= 6

	# You can chain tools.
	Scenario: We need to get letter statistics from the results of the WordCountTool
        Given these text records in the workflow "input" directory:
            | abc abc |
            | de |
        
        # Note that we set the output of the WordCountTool to an explicit directory.
        And the workflow parameter "output" is "build/test/WordCountTool/input_for_LetterCount"
        And the workflow is run
        Then the workflow "output" result should only have records where:
        	| word | count |
        	| abc | 2 |
        	| de | 1 |
        
        # And now we continue with the next sequence of steps, which runs a different workflow
		Given the com.scaleunlimited.cascading.cuke package contains the LetterCountTool workflow
		And the workflow will be run locally with test directory "build/test/LetterCountTool"
		And these parameters for the workflow:
			| input | build/test/WordCountTool/input_for_LetterCount |
			| output | ${testdir}/output |
		When the workflow is run
		Then the workflow "output" result should have records where:
        	| letter | count |
        	| a | 2 |
        	| b | 2 |
        	| c | 2 |
        	| d | 1 |
        	| e | 1 |
	
	Scenario: We want to be able to run the LetterCount workflow with specific data
		Given the com.scaleunlimited.cascading.cuke package contains the LetterCountTool workflow
		And the workflow will be run locally with test directory "build/test/LetterCountTool/"
		And these parameters for the workflow:
			| input | ${testdir}/input |
			| output | ${testdir}/output |
	
		# We're creating input data, where we're specifying the fields and values for
		# a list of records (tuples).
		And these "word count" records in the workflow "input" directory:
			| word | count |
			| super | 10 |
			| racer | 100 |
		When the workflow is run
		Then the workflow "output" result should only have records where:
			| letter | count |
			| s | 10 |
			| u | 10 |
			| p | 10 |
			| e | 110 |
			| r | 210 |
			| a | 100 |
			| c | 100 |
			
	Scenario: We want to be able to run the LetterCount workflow with randomized data
		Given the com.scaleunlimited.cascading.cuke package contains the LetterCountTool workflow
		And the workflow will be run locally with test directory "build/test/LetterCountTool/"
		And these parameters for the workflow:
			| input | ${testdir}/input |
			| output | ${testdir}/output |
			
		# When we specify an explicit number of records, and we've got a list of field/value
		# settings, then we'll create that many records by randomly picking from the list.
		And 100 "word count" records in the workflow "input" directory:
			| word | count |
			| super | 10 |
			| racer | 100 |
		When the workflow is run
		Then the workflow "output" result should only have records where:
			| letter |
			| s |
			| u |
			| p |
			| e |
			| r |
			| a |
			| c |
	
	Scenario: We want to be able to run the LetterCount workflow with mock data
		Given the com.scaleunlimited.cascading.cuke package contains the LetterCountTool workflow
		And the workflow will be run locally with test directory "build/test/LetterCountTool/"
		And these parameters for the workflow:
			| input | ${testdir}/input |
			| output | ${testdir}/output |
		
		# If we don't specify all of the fields in the tuple (here we're just setting the word,
		# not the count) then mock (random) values will be used for all of the unspecified fields.
		And 10 "word count" records in the workflow "input" directory:
			| word |
			| super |
			| racer |
		When the workflow is run
		Then the workflow "output" result should only have records where:
			| letter |
			| s |
			| u |
			| p |
			| e |
			| r |
			| a |
			| c |
	
</pre>
