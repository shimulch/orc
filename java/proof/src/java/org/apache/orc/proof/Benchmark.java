package org.apache.orc.proof;

import org.apache.commons.cli.*;
import org.apache.orc.learned.IndexManager;

import java.io.IOException;
import java.net.URISyntaxException;

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
        System.out.println("Arguments: " + args.toString());
        converter.writeFromRefData(args[1], args[2], args[3]);
    }

    private static void index(String[] args) throws IOException, URISyntaxException {
        IndexManager manager = new IndexManager();
        manager.index(args[1], Integer.parseInt(args[2]));
    }

    private static void search(String[] args) throws IOException, URISyntaxException {
        IndexManager manager = new IndexManager();
        manager.filter(args[1], Float.parseFloat(args[2]));
    }
}
