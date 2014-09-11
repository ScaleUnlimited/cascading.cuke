package com.scaleunlimited.cascading.cuke;

import gherkin.parser.Parser;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.regex.RegexGenerator;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.LocalPlatform;


public class CascadingFormatterTest {
	
	public static final String TEST_DIR = "build/test/CascadingFormatterTest/";
	public static final String INPUT_TEXT_DIRNAME = "input-text";
	
	private enum WordCountCounters {
		TOTAL_WORDS,
		FREQUENCY_BY_WORD,
	}
	
	@SuppressWarnings({ "serial", "rawtypes" }) 
	public static class CounterFilter 
	    extends BaseOperation<NullContext>
	    implements Filter<NullContext> {
	    
	    private String _counterGroupName;
	    private String _counterNameFieldName;

	    public CounterFilter(   String counterGroupName,
	                            String counterNameFieldName) {
	        _counterGroupName = counterGroupName;
	        _counterNameFieldName = counterNameFieldName;
	    }

	    @Override
	    public boolean isRemove(FlowProcess flowProcess, FilterCall<NullContext> filterCall) {
            String counterGroupKey = filterCall.getArguments().getString(_counterNameFieldName);
            flowProcess.increment(_counterGroupName, counterGroupKey, 1);
            flowProcess.increment(WordCountCounters.TOTAL_WORDS, 1);
            return false;
	    }
	}

	public static class WordCountSD extends WorkflowSD {
		private static final String WORD_BREAK_PATTERN_STRING
			= "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";

		public WordCountSD() {
			super("WordCountTool");
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public Flow createFlow() {
			
			try {
				BasePlatform platform = 
					new LocalPlatform(CascadingFormatterTest.class);
				
				String inputText = 
					((SimpleSSE<String>)(_scenarioState.get("inputText"))).getValue();
				writeInputText(inputText);
				BasePath inputTextPath =
					platform.makePath(TEST_DIR + INPUT_TEXT_DIRNAME);
				Tap inputTextSource =
					platform.makeTap(	platform.makeTextScheme(),
										inputTextPath);
				
				Pipe wordPipe = new Pipe("words");
				wordPipe = new Each(wordPipe, 
									new Fields("line"), 
									new RegexGenerator(	new Fields("word"),
														WORD_BREAK_PATTERN_STRING));
				wordPipe = new GroupBy(wordPipe, new Fields("word"));
				WordCountCounters.class.getSimpleName();
				CounterFilter counterFilter 
					= new CounterFilter((	WordCountCounters.class.getSimpleName()
										+	"."
										+	WordCountCounters.FREQUENCY_BY_WORD.name()),
										"word");
				wordPipe = new Each(wordPipe,
									counterFilter);
				FlowConnector flowConnector = platform.makeFlowConnector();
				return flowConnector.connect(	"Counting words",
												inputTextSource,
												new NullSinkTap(),
												wordPipe);
			} catch (Exception e) {
				throw new RuntimeException("Can't create flow", e);
			}
		}
    }
	
	@Test
	public void testWordCountFlow() throws Throwable {
		CascadingFormatter formatter =
			new CascadingFormatter(System.out, false, true);
		
		formatter.addStepDefinition(new WorkflowParameterSD());
		formatter.addStepDefinition(new WordCountSD());
		formatter.addStepDefinition(new WorkflowCounterAssertionSD());
		
		StringBuilder featureSource = new StringBuilder();
		featureSource.append("Feature: WordCountTool\n");
		featureSource.append("Scenario: Correctly computes word frequencies\n");
		featureSource.append("When the WordCountTool workflow is run\n");
		featureSource.append("And the inputText parameter is Now is the time for all good men to come to the aid of their country.\n");
		featureSource.append("Then the WordCountTool com.scaleunlimited.cascading.cuke.CascadingFormatterTest$WordCountCounters.TOTAL_WORDS counter value is 16\n");
		featureSource.append("Then the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.the counter value is 2\n");
		featureSource.append("Then the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.to counter value is at least 2\n");
		featureSource.append("Then the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.good counter value is >=1\n");
		featureSource.append("Then the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.their counter value is at most 1\n");
		featureSource.append("Then the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.men counter value is greater than 0\n");
		featureSource.append("Then the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.come counter value is less than 2\n");
        Parser parser = new Parser(formatter);
		parser.parse(featureSource.toString(), "", 0);
        formatter.close();
	}
	
	private static void writeInputText(String inputText) {
		Writer writer = null;
		
		File testDir = new File(TEST_DIR);
		testDir.mkdirs();
		File inputTextFile = new File(testDir, INPUT_TEXT_DIRNAME);
        try {
	        FileOutputStream fos =
	            new FileOutputStream(inputTextFile.toString());
	        writer = new OutputStreamWriter(fos);
        	writer.write(inputText + "\r");
        } catch (IOException e) {
        	throw new RuntimeException("Unable to write input text to " + inputTextFile);
		} finally {
        	if (writer != null) {
            	try {
					writer.close();
				} catch (IOException e) {
		        	throw new RuntimeException("Unable to write input text to " + inputTextFile);
				}
        	}
        }
	}
}
