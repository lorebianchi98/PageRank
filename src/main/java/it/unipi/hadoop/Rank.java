package it.unipi.hadoop;

import it.unipi.hadoop.writable.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Rank {
    private String input;
    private String output;
    private String baseOutput;
    private Integer iteration;
    private Integer pageCount;
    private double alpha;
    //private final int numReducers;
    private static final String OUTPUT_PATH = "/rank";

    public Rank(String input, String baseOutput, Integer iteration, Integer pageCount, double alpha) {
        this.input = input;
        this.baseOutput = baseOutput;
        this.output = baseOutput + OUTPUT_PATH + "_" + iteration;
        this.iteration = iteration;
        this.pageCount = pageCount;
        this.alpha = alpha;
    }

    //prepare the rank object for the next iteration
    public void iterate(){
        iteration++;
        input = output;
        output = baseOutput + OUTPUT_PATH + "_" + iteration;
    }

    public String getOutput(){
        return output;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public Integer getIteration() {
        return iteration;
    }

    public void setIteration(Integer iteration) {
        this.iteration = iteration;
    }

    public static class RankMapper extends Mapper<Text, Text, Text, Node> {
        private static final Text keyOut = new Text();
        private static final Node valueOut = new Node();
        private static final List<String> empty = new ArrayList<>();

        private static List<String> outLinks;
        private static double mass;

        // For each line of the input (page title and its node features)
        // (1) emit page title and its node features to maintain the graph structure
        // (2) emit out-link pages with their mass (rank share)
        @Override
        public void map(final Text keyIn, final Text valueIn, final Context context) throws IOException, InterruptedException {
            keyOut.set(keyIn.toString());
            valueOut.setFromJson(valueIn.toString());
            context.write(keyOut, valueOut); // (1)

            outLinks = valueOut.getAdjacencyList();
            mass = valueOut.getPageRank() / outLinks.size();

            valueOut.setAdjacencyList(empty);
            valueOut.setIsNode(false);
            for(String outLink: outLinks) {
                keyOut.set(outLink);
                valueOut.setPageRank(mass);
                valueOut.setTitle(outLink);
                context.write(keyOut, valueOut); // (2)
            }
        }
    }

    public static class RankCombiner extends Reducer<Text, Node, Text, Node> {
        private static final Node outValue = new Node();
        private static final List<String> empty = new LinkedList<>();

        private static double aggregatedRank;

        // For each key pass the graph structure (1) and sum up the aggregate ranking (2)
        @Override
        public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
            aggregatedRank = 0.0;
            outValue.setAdjacencyList(empty);
            outValue.setIsNode(false);

            for(Node p: values) {
                if(p.isNode())
                    context.write(key, p); // (1)
                else
                    aggregatedRank += p.getPageRank();
            }
            outValue.setTitle(key.toString());
            outValue.setPageRank(aggregatedRank);
            context.write(key, outValue); // (2)
        }
    }

    public static class RankReducer extends Reducer<Text, Node, Text, Node> {
        private static double alpha;
        private static int pageCount;
        private static final Node valueOut = new Node();
        private static final List<String> empty = new LinkedList<>();

        private static double rank;
        private static double newPageRank;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            alpha = context.getConfiguration().getDouble("alpha", 0);
            pageCount = context.getConfiguration().getInt("page.count", 0);
        }

        // For each node object
        // (1) if it is a node, recover the structure from it
        // (2) else, get from it an incoming rank contribution
        @Override
        public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
            rank = 0.0;
            valueOut.setAdjacencyList(empty);
            valueOut.setIsNode(false);

            for(Node node: values) {
                if(node.isNode())
                    valueOut.setFromNode(node);  // (1)
                else
                    rank += node.getPageRank(); // (2)
            }
            newPageRank = (alpha / ((double)pageCount)) + ((1 - alpha) * rank);
            valueOut.setPageRank(newPageRank);
            valueOut.setTitle(key.toString());
            context.write(key, valueOut);
        }
    }

    public boolean run(int numReducers) throws Exception {
        // set configurations
        final Configuration conf = new Configuration();

        // instantiate job
        final Job job = new Job(conf, "Rank-" + iteration);
        job.setJarByClass(Rank.class);

        // set mapper/combiner/reducer
        job.setMapperClass(RankMapper.class);
        job.setCombinerClass(RankCombiner.class);
        job.setReducerClass(RankReducer.class);

        // set the random jump probability alpha and the page count
        job.getConfiguration().setDouble("alpha", alpha);
        job.getConfiguration().setInt("page.count", pageCount);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);

        // set the number of reducer tasks to be used
        job.setNumReduceTasks(numReducers);

        // define I/O
        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // define input/output format
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }
}
