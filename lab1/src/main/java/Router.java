import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import javax.xml.stream.XMLStreamException;
import java.io.*;
import java.lang.*;


public class Router {
    private static final String DEFAULT_INPUT_FILE_NAME = "input.bz2";
    private static final String DEFAULT_OUTPUT_FILE_NAME = "output.xml";
    private static final String DEFAULT_NUMBER_OF_SYMBOLS_TO_READ = "5000000";
    private static final String DEFAULT_DATABASE = "jdbc:postgresql://localhost:5432/";
    private static final String DEFAULT_USER_NAME = "postgres";
    private static final String DEFAULT_PASSWORD = "1234";
    private static final String DEFAULT_TABLE_NAME = "node_json";
    private static final String DEFAULT_METHOD_NAME = "batch";

    private static final Logger logger = LogManager.getLogger(Router.class);

    public static void main(String[] args) throws ParseException {
        logger.info("start");

        CommandLine commandLine = createCommandLine(args);
        String archiveFileName = commandLine.getOptionValue("i", DEFAULT_INPUT_FILE_NAME);
        String outputFileName = commandLine.getOptionValue("o", DEFAULT_OUTPUT_FILE_NAME);
        long num = Long.parseLong(commandLine.getOptionValue('n', DEFAULT_NUMBER_OF_SYMBOLS_TO_READ));
        var jdbcURL = commandLine.getOptionValue('d', DEFAULT_DATABASE);
        var userName = commandLine.getOptionValue('u', DEFAULT_USER_NAME);
        var userPassword = commandLine.getOptionValue('p', DEFAULT_PASSWORD);
        var tableName = commandLine.getOptionValue('t', DEFAULT_TABLE_NAME);
        var methodName = commandLine.getOptionValue('m', DEFAULT_METHOD_NAME);

        var inserter = new DataBaseInserter(archiveFileName, num, jdbcURL, userName, userPassword);
        inserter.insertToDatabaseSimple(tableName, methodName);
        logger.info("end");
    }

    private static CommandLine createCommandLine(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("i", "input", true, "input file path");
        options.addOption("o", "output", true, "output file path");
        options.addOption("n", "number", true, "number of symbols to read");
        options.addOption("d", "database", true, "Database");
        options.addOption("t", "table", true, "node | node_json | node_ctype");
        options.addOption("m", "method", true, "insert | prepared statement | batch");
        options.addOption("u", "user", true, "JDBC username");
        options.addOption("p", "password", true, "JDBC password");
        DefaultParser commandLineParser = new DefaultParser();
        return commandLineParser.parse(options, args);
    }
}
