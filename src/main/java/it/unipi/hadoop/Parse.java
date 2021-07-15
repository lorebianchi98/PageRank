package it.unipi.hadoop;

import it.unipi.hadoop.util.PageParser;
import it.unipi.hadoop.writable.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Parse {
    private String input;
    private String output;
    //private final int numReducers;
    private int pageCount;
    private static final String PATH = "/parse";

    public Parse(String input, String output, int pageCount) {
        this.input = input;
        this.output = output + PATH;
        //this.numReducers = numReducers;
        this.pageCount = pageCount;
    }

    public String getOutput() {
        return output;
    }

    //takes as input a line of the input file and emit key-value pairs (title, out-link), for each out-link
    public static class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text keyOut = new Text();
        private static final Text valueOut = new Text();
        private static final Text empty = new Text("");
        private static final PageParser pageParser = new PageParser();

        private static String title;
        private static List<String> outLinks;

        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            pageParser.setPage(value.toString());
            title = pageParser.getTitle();
            outLinks = pageParser.getOutLinks();
            if(title != null) {
                keyOut.set(title);
                if(outLinks.size() > 0) {
                    for (String outLink : outLinks) {
                        valueOut.set(outLink);
                        context.write(keyOut, valueOut);
                        context.write(valueOut, empty);
                    }
                } else
                    context.write(keyOut, empty); //node without any out-links
            }
        }
    }

    //takes as input key-value pairs (title, out-link) and emit (title, node feature)
    public static class ParseReducer extends Reducer<Text, Text, Text, Node> {
        private static int pageCount;
        private static final Node valueOut = new Node();

        private static List<String> adjacencyList;
        private static String value;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //from the configuration it is read the output from the map reduce job page count
            pageCount = context.getConfiguration().getInt("page.count", 0);
        }

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            adjacencyList = new ArrayList<>();
            for(Text outLink: values) {
                value = outLink.toString();
                if(!value.equals(""))
                    adjacencyList.add(value);
            }
            valueOut.setTitle(key.toString());
            valueOut.setAdjacencyList(adjacencyList);
            valueOut.setPageRank(1.0d/pageCount);
            valueOut.setIsNode(true);
            context.write(key, valueOut);
        }
    }

    public boolean run() throws Exception{
        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "parser");
        job.setJarByClass(Parse.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //set the pageCount on the configuration
        job.getConfiguration().setInt("page.count", pageCount);

        job.setMapperClass(ParseMapper.class);
        job.setReducerClass(ParseReducer.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true);
    }

}
