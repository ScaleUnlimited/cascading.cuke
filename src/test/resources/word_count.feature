Feature: WordCountTool
Scenario: Correctly computes word frequencies
When the WordCountTool workflow is run
And the inputText parameter is Now is the time for all good men to come to the aid of their country.
		
# These should all pass:
Then the WordCountTool com.scaleunlimited.cascading.cuke.stepdefinitions.WordCountSD$WordCountCounters.TOTAL_WORDS counter value is 16
Then the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.the counter value is 2
Then the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.to counter value is at least 2
Then the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.good counter value is >=1
Then the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.their counter value is at most 1
		
# This one should fail (as there's only one instance of "men"):
Then the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.men counter value is greater than 1
		
# This one should pass:
Then the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.come counter value is less than 2
		
# This one has no definition:
Then this undefined assertion would still need to be implemented
		
# This one would pass, but it gets skipped because the previous one 
# was undefined:
Then the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.men counter value is 1
