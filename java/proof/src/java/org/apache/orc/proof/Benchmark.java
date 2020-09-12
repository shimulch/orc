package org.apache.orc.proof;

import org.apache.commons.cli.*;

import java.io.IOException;

public class Benchmark {

    public static void main(String[] args) throws ParseException, IOException, org.json.simple.parser.ParseException {
        Options options = new Options();
        options.addOption("convert", true, "Convert from tpc-h data to orc");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if(cmd.hasOption("convert")) {
            convert(args);
        }
    }

    private static void convert(String[] args) throws IOException, org.json.simple.parser.ParseException {
        TPCDataConverter converter = new TPCDataConverter();
        converter.writeFromRefData(args[0], args[1], args[2]);
    }
}
