package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        /*
        if(otherArgs.length != 5) {
            System.err.println("Usage: PageRank <input> <base output> <# of iterations> <# of reducers> <random jump probability>");
            System.exit(1);
        }
        */
        final String INPUT = args[0];
        final String BASE_OUTPUT = args[1];

        //delete output directory if it exists already
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(BASE_OUTPUT))){
            fs.delete(new Path(BASE_OUTPUT), true);
            System.out.println("Old output directory deleted");
        }

        // Count Stage
        Count countStage = new Count(INPUT, BASE_OUTPUT);
        if(countStage.run()) {
            throw new Exception("Count job failed");
        }
        int totalPages = countStage.getTotalPages();
        System.out.println(">> Count Stage completed. Total pages = " + totalPages);

        // Parse Stage
        Parse parseStage = new Parse(INPUT, BASE_OUTPUT, totalPages);
        if(parseStage.run()) {
            throw new Exception("Parse job failed");
        }
        System.out.println(">> Parse Stage completed");

    }
}
