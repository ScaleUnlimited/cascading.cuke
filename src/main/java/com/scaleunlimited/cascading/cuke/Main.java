package com.scaleunlimited.cascading.cuke;

import gherkin.parser.Parser;
import gherkin.util.FixJava;

import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.scaleunlimited.cascading.cuke.stepdefinitions.WordCountSD;
import com.scaleunlimited.cascading.cuke.stepdefinitions.WorkflowCounterAssertionSD;
import com.scaleunlimited.cascading.cuke.stepdefinitions.WorkflowParameterSD;


public class Main {
	private FileFilter featureFilter = new FileFilter() {
		public boolean accept(File file) {
			return file.isDirectory() || file.getName().endsWith(".feature");
		}
	};
	private Parser parser;
	private final Writer out;
	public Main(final Writer out) {
		this.out = out;
		final CascadingFormatter formatter = new CascadingFormatter(out, false, true);
		formatter.addStepDefinition(new WorkflowParameterSD());
		formatter.addStepDefinition(new WordCountSD());
		formatter.addStepDefinition(new WorkflowCounterAssertionSD());
		parser = new Parser(formatter);
	}
	private void scanAll(File file) throws IOException {
		walk(file);
		out.append('\n');
		out.close();
	}
	private void walk(File file) {
		if (file.isDirectory()) {
			for (File child : file.listFiles(featureFilter)) {
				walk(child);
			}
		} else {
			parse(file);
		}
	}
	private void parse(File file) {
		try {
			String input = FixJava.readReader(new FileReader(file));
			parser.parse(input, file.getPath(), 0);
		} catch (Exception e) {
			e.printStackTrace(System.err);
			System.exit(1);
		}
	}
	public static void main(String[] args) throws IOException {
		new Main(new OutputStreamWriter(System.out, "UTF-8")).scanAll(new File(args[0]));
	}
}
