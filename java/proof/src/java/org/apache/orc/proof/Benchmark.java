package org.apache.orc.proof;

import org.apache.commons.cli.*;
import org.apache.commons.lang.ArrayUtils;
import org.apache.orc.learned.IndexManager;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;

public class Benchmark {

    public static void main(String[] args) throws ParseException, IOException, org.json.simple.parser.ParseException, URISyntaxException {
        Options options = new Options();
        options.addOption("convert", true, "Convert from tpc-h data to orc. <data-dir> <schema.json> <output-file>");
        options.addOption("index", true, "Index an Orc file. <orc-file> <column-num-to-index>");
        options.addOption("search", true, "Search a value from orc file using learned index. <orc-file> <value-to-find>");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if(cmd.hasOption("convert")) {
            convert(args);
        } else if (cmd.hasOption("index")) {
            index(args);
        } else if (cmd.hasOption("search")) {
            search(args);
        } else {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("proof", options);
        }
    }

    private static void convert(String[] args) throws IOException, org.json.simple.parser.ParseException {
        TPCDataConverter converter = new TPCDataConverter();
        System.out.println("Arguments: " + args.toString());
        converter.writeFromRefData(args[1], args[2], args[3]);
    }

    private static void index(String[] args) throws IOException, URISyntaxException {
        IndexManager manager = new IndexManager();
        Integer[] stages;
        if(args.length == 4) {
            stages = (Integer[]) Arrays.stream(args[3].split(",")).map(Integer::parseInt).toArray();
        } else {
            stages = new Integer[]{10, 100, 1000};
        }
        manager.index(args[1], Integer.parseInt(args[2]), ArrayUtils.toPrimitive(stages));
    }

    private static void search(String[] args) throws IOException, URISyntaxException {
        IndexManager manager = new IndexManager();
        manager.filter(args[1], Float.parseFloat(args[2]));
    }
}
