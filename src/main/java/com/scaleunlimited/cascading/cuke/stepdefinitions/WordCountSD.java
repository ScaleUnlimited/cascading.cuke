package com.scaleunlimited.cascading.cuke.stepdefinitions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

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
import com.scaleunlimited.cascading.cuke.Main;
import com.scaleunlimited.cascading.cuke.SimpleSSE;
import com.scaleunlimited.cascading.local.LocalPlatform;

public class WordCountSD extends WorkflowSD {
	public static final String WORKING_DIRNAME = "build/test/CascadingFormatterTest/";
	
	public static final String INPUT_TEXT_DIRNAME = "input-text";

	private static final String WORD_BREAK_PATTERN_STRING
		= "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";

	enum WordCountCounters {
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

	public WordCountSD() {
		super("WordCountTool");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Flow createFlow() {
		
		try {
			BasePlatform platform = 
				new LocalPlatform(Main.class);
			
			String inputText = 
				((SimpleSSE<String>)(_scenarioState.get("inputText"))).getValue();
			writeInputText(inputText);
			BasePath workingPath =
				platform.makePath(WORKING_DIRNAME);
			BasePath inputTextPath =
				platform.makePath(workingPath, INPUT_TEXT_DIRNAME);
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
	
	static void writeInputText(String inputText) {
		Writer writer = null;
		
		File testDir = new File(WORKING_DIRNAME);
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