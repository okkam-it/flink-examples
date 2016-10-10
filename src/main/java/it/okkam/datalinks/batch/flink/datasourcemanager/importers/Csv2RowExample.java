package it.okkam.datalinks.batch.flink.datasourcemanager.importers;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.utils.Option;
import org.apache.flink.api.java.utils.OptionType;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.utils.RequiredParameters;
import org.apache.flink.api.java.utils.RequiredParametersException;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.sources.CsvTableSource;
import org.apache.flink.util.Collector;

public class Csv2RowExample {

	public static final String PARAM_CSV_FILE_PATH = "csvFilePath";
	public static final String PARAM_HEADER = "header";
	public static final String PARAM_FIELD_TYPES = "fieldTypes";
	public static final String PARAM_FIELD_DELIM = "fieldDelim";
	public static final String PARAM_LINE_DELIM = "lineDelim";
	public static final String PARAM_QUOTE_CHAR = "quote";
	public static final String PARAM_IGNORE_COMMENTS = "ignoreComments";
	public static final String PARAM_IGNORE_FIRST_LINE = "ignoreFirstLine";

	// variable params
	protected static String dataSetId;
	protected static String csvFilePath;
	private static String[] fieldNames;
	protected static TypeInformation<?>[] fieldTypes;
	protected static Character quoteCharacter = '"';
	protected static String ignoreComments;
	protected static String fieldDelim = CsvInputFormat.DEFAULT_FIELD_DELIMITER;
	protected static String lineDelim = CsvInputFormat.DEFAULT_LINE_DELIMITER;
	protected static Boolean ignoreFirstLine = Boolean.TRUE;
	protected static boolean lenient = Boolean.FALSE;

	protected static URI outputPathUri;
	private static String headerStr;

	public static void main(String[] args) throws Exception {
		boolean parseWithApacheCommonsCsv = true;
		String csvFilePath = "/tmp/test.csv";
		List<String> csvLines = new ArrayList<>();
		csvLines.add("Column1,Column2");
		csvLines.add("val1,val2");
		csvLines.add("test,test");
		// the following cause an exception with current Flink CsvParser (1.1.2)
		csvLines.add("test,\"a field with a \"\"quoted\"\" value\"");
		FileUtils.deleteQuietly(new File(csvFilePath));
		FileUtils.writeLines(new File(csvFilePath), csvLines);
		args = new String[] { 
				"--" + PARAM_CSV_FILE_PATH, "file:" + csvFilePath, 
				"--" + PARAM_HEADER, csvLines.get(0),
				"--" + PARAM_IGNORE_FIRST_LINE, Boolean.TRUE.toString(), };

		parseParameters(args);

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Row> csvDs = null;
		if (parseWithApacheCommonsCsv) {
			// can be removed when Table API will be fixed
			// => reads all fields as 'string' up to now..
			org.apache.flink.core.fs.Path filePath = new org.apache.flink.core.fs.Path(csvFilePath);
			TextInputFormat inputFormat = new TextInputFormat(filePath);
			DataSet<String> lines = env.readFile(inputFormat, csvFilePath);
			csvDs = lines.flatMap(new RawCsv2Row(headerStr));
		} else {
			// Table API (doesn't parse string fields containing double quotes)
			// => https://issues.apache.org/jira/browse/FLINK-4785
			CsvTableSource csvTableSource = new CsvTableSource(csvFilePath, fieldNames, fieldTypes, fieldDelim,
					lineDelim, quoteCharacter, ignoreFirstLine, ignoreComments, lenient);
			csvDs = csvTableSource.getDataSet(env);
		}

		csvDs.print();

	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************
	private static void parseParameters(String[] args) throws RequiredParametersException {
		ParameterTool parameters = ParameterTool.fromArgs(args);
		RequiredParameters required = new RequiredParameters();

		required.add(new Option(PARAM_FIELD_TYPES).defaultValue("")
				.help("The list of column types (string,int,double or boolean). Default is 'string' for all columns")
				.type(OptionType.STRING));
		required.add(new Option(PARAM_FIELD_DELIM).defaultValue(CsvInputFormat.DEFAULT_FIELD_DELIMITER)
				.help("Delimiter field delimiter. Default is " + CsvInputFormat.DEFAULT_FIELD_DELIMITER)
				.type(OptionType.STRING));
		required.add(new Option(PARAM_LINE_DELIM).defaultValue(CsvInputFormat.DEFAULT_LINE_DELIMITER)
				.help("Delimiter line delimiter. Default is " + CsvInputFormat.DEFAULT_LINE_DELIMITER)
				.type(OptionType.STRING));
		required.add(new Option(PARAM_QUOTE_CHAR).defaultValue(quoteCharacter.toString())
				.help("Quote character. Default is " + quoteCharacter).type(OptionType.STRING));
		required.add(new Option(PARAM_IGNORE_FIRST_LINE).defaultValue(ignoreFirstLine.toString())
				.help("Ignore first line or not. Default is " + ignoreFirstLine).type(OptionType.BOOLEAN));
		required.add(new Option(PARAM_HEADER).defaultValue(quoteCharacter.toString())
				.help("The header information (separated by the delimiter character). Used to extract the column names")
				.type(OptionType.STRING));
		required.add(new Option(PARAM_IGNORE_COMMENTS).defaultValue("")
				.help("The character denoting a comment line. Default is none (no parameter set)")
				.type(OptionType.STRING));

		required.applyTo(parameters);

		headerStr = parameters.getRequired(PARAM_HEADER);
		fieldNames = headerStr.split(fieldDelim);

		fieldDelim = parameters.getRequired(PARAM_FIELD_DELIM);

		String fieldTypesStr = parameters.getRequired(PARAM_FIELD_TYPES);
		fieldTypes = getFieldTypes(fieldNames.length, fieldTypesStr, fieldDelim);
		csvFilePath = parameters.getRequired(PARAM_CSV_FILE_PATH);
		lineDelim = parameters.getRequired(PARAM_LINE_DELIM);
		quoteCharacter = parameters.getRequired(PARAM_QUOTE_CHAR).charAt(0);
		ignoreFirstLine = parameters.getBoolean(PARAM_IGNORE_FIRST_LINE);
		ignoreComments = parameters.getRequired(PARAM_IGNORE_COMMENTS);

		if (ignoreComments.isEmpty()) {
			ignoreComments = null;
		}
		// CoC - convention over configuration :)

		System.out.println("------------------------------------------------------");
		System.out.println(" CSV reader");
		System.out.println("------------------------------------------------------\n");

		System.out.printf("\t- dataSetId: %s\n", dataSetId);
		System.out.printf("\t- csvFilePath: %s\n", csvFilePath);
		System.out.printf("\t- fieldDelim: %s\n", fieldDelim);
		System.out.printf("\t- lineDelim: %s\n", lineDelim);
		System.out.printf("\t- quoteCharacter: %s\n", quoteCharacter);
		System.out.printf("\t- header: %s\n", headerStr);
		System.out.printf("\t- ignoreFirstLine: %s\n", ignoreFirstLine);
		System.out.printf("\t- ignoreComments: %s\n", ignoreComments);
		System.out.println("------------------------------------------------------");
	}

	public static TypeInformation<?>[] getFieldTypes(int length, String fieldTypesStr, String fieldDelim) {
		TypeInformation<?>[] ret = new TypeInformation<?>[length];
		// default: string for all columns
		if (fieldTypesStr == null || fieldTypesStr.isEmpty()) {
			for (int i = 0; i < ret.length; i++) {
				ret[i] = BasicTypeInfo.STRING_TYPE_INFO;
			}
			return ret;
		}
		String[] split = fieldTypesStr.split(fieldDelim);
		for (int i = 0; i < split.length; i++) {
			if (split[i].equals("string")) {
				ret[i] = BasicTypeInfo.STRING_TYPE_INFO;
			} else if (split[i].equals("int")) {
				ret[i] = BasicTypeInfo.INT_TYPE_INFO;
			} else if (split[i].equals("double")) {
				ret[i] = BasicTypeInfo.DOUBLE_TYPE_INFO;
			} else if (split[i].equals("boolean")) {
				ret[i] = BasicTypeInfo.BOOLEAN_TYPE_INFO;
			} else {
				throw new IllegalArgumentException("Cannot get Flink type info for " + split[i]);
			}
		}
		return ret;
	}

	private static class RawCsv2Row extends RichFlatMapFunction<String, Row> {

		private static final long serialVersionUID = 1L;

		private transient Row reuse;
		private transient CSVFormat csvFormat;

		private String headerStr;
		private String[] header;

		public RawCsv2Row(String headerStr) {
			this.headerStr = headerStr;
			this.header = headerStr.split(Pattern.quote(","));
		}

		@Override
		public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
			csvFormat = CSVFormat
					.newFormat(',')
					.withQuote('"')
					.withQuoteMode(QuoteMode.MINIMAL)
					.withNullString("")
					.withHeader(header).withIgnoreSurroundingSpaces(true);
			this.reuse = new Row(header.length);
		}

		@Override
		public void flatMap(String line, Collector<Row> out) throws Exception {
			// skip header
			if (line.equals(headerStr)) {
				return;
			}
			// subjects.clear();
			List<CSVRecord> rowList;
			try {
				rowList = CSVParser.parse(line, csvFormat).getRecords();
			} catch (IOException ex) {
				throw new IOException("Invalid line: " + line, ex);
			}
			for (CSVRecord row : rowList) {
				if (row.size() != header.length) {
					throw new IOException("Invalid line lenght (current: " + row.size() + ", expected: " + header.length
							+ ") for line " + line);
				}
				for (int i = 0; i < header.length; i++) {
					reuse.setField(i, row.get(i));
				}
				out.collect(reuse);
			}
		}

	}

}
