package it.unipi.hadoop;

import it.unipi.hadoop.util.PageParser;
import it.unipi.hadoop.writable.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
    private final String INPUT;
    private final String BASE_OUTPUT;
    //private final int numReducers;
    private final int PAGE_COUNT;

    public Parse(String INPUT, String BASE_OUTPUT, int PAGE_COUNT) {
        this.INPUT = INPUT;
        this.BASE_OUTPUT = BASE_OUTPUT;
        //this.numReducers = numReducers;
        this.PAGE_COUNT = PAGE_COUNT;
    }

    //takes as input a line of the input file and emit key-value pairs (title, out-link), for each out-link
    public static class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text keyOut = new Text();
        private static final Text valueOut = new Text();
        private static final Text empty = new Text("");
        private static final PageParser pageParser = new PageParser();

        private static String title;
        private static List<String> outLinks;
        private Logger logger = Logger.getLogger(ParseMapper.class);

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
                    }
                } else
                    context.write(keyOut, empty); //node without any out-links
            }
        }
    }

    //takes as input key-value pairs (title, out-link) and emit (title, node feature)
    public static class ParseReducer extends Reducer<Text, Text, Text, Node> {
        private int pageCount;
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
            valueOut.setAdjacencyList(adjacencyList);
            valueOut.setPageRank(1.0d/pageCount);
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
        job.getConfiguration().setInt("page.count", PAGE_COUNT);

        job.setMapperClass(ParseMapper.class);
        job.setReducerClass(ParseReducer.class);

        FileInputFormat.addInputPath(job, new Path(INPUT));
        FileOutputFormat.setOutputPath(job, new Path(BASE_OUTPUT));

        return job.waitForCompletion(true);
    }

}
