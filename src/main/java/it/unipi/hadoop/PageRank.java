package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PageRank {
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();

        if(args.length != 5) {
            System.err.println("Invalid parameter lists: <input> <output directory> <# of iterations> <# of reducers> <random jump probability>");
            System.exit(1);
        }
        final String INPUT = args[0];
        final String BASE_OUTPUT = args[1];
        final int ITERATIONS = Integer.parseInt(args[2]);
        final int NUM_REDUCERS = Integer.parseInt(args[3]);
        final double ALPHA = Double.parseDouble(args[4]);

        //delete output directory if it exists already
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(BASE_OUTPUT))){
            fs.delete(new Path(BASE_OUTPUT), true);
            System.out.println("Old output directory deleted");
        }
        //creating the base output directory
        fs.mkdirs(new Path(BASE_OUTPUT));

        // Count Stage
        Count countStage = new Count(INPUT, BASE_OUTPUT);
        if(!countStage.run()) {
            throw new Exception("Count job failed");
        }
        int totalPages = countStage.getTotalPages();
        System.out.println("COUNT STAGE COMPLETED. Total pages = " + totalPages);

        // Parse Stage
        Parse parseStage = new Parse(INPUT, BASE_OUTPUT, totalPages);
        if(!parseStage.run(NUM_REDUCERS)) {
            throw new Exception("Parse job failed");
        }
        System.out.println("PARSE STAGE COMPLETED");

        // Rank Stage for NUM_ITERATIONS
        String nextInput = parseStage.getOutput();
        Rank rankStage = new Rank(nextInput, BASE_OUTPUT, 0, totalPages, ALPHA);
        for(int i = 0; i < ITERATIONS; i++) {
            if(!rankStage.run(NUM_REDUCERS))
                throw new Exception("Rank " + i + "-th job failed");
            System.out.println("ITERATION " + i + " COMPLETED");
            nextInput = rankStage.getOutput();
            rankStage.iterate();
        }
        System.out.println("RANK STAGE COMPLETED");

        Sort sortStage = new Sort(nextInput, BASE_OUTPUT);
        if(!sortStage.run())
            throw new Exception("Sort job failed");
        System.out.println("SORT STAGE COMPLETED");
    }
}
