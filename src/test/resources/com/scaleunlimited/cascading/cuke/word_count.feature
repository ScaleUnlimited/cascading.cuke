Feature: Count words
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
        # Make sure we can handle fully-qualified counters
        And the workflow "WordCountCounters.WORDS" counter should be < 7
        # Make sure we can handle long values (greater than 2 billion+), so try with 10B.
        And the workflow "WORDS" counter should be less than 10000000000
        And the workflow "WORDS" counter should be at most 6
        And the workflow "WORDS" counter should be <= 6
        
	# TODO add a workflow to sort the results, so input is a tuple
	# TODO add scenarios that support mock data generation, validation of results
	# TODO add scenario to test random text generation
	
	