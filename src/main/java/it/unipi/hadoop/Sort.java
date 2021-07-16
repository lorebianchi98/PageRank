package it.unipi.hadoop;

import it.unipi.hadoop.writable.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
public class Sort {
    private static final String OUTPUT_PATH = "/sort";
    private String output;
    private String input;


    public Sort(String input, String baseOutput){
        this.output = baseOutput + OUTPUT_PATH;
        this.input = input;
    }

    public static class SortMapper extends Mapper<Text, Text, Node, NullWritable> {
        private static final Node node = new Node();
        private static final Node keyOut = new Node();
        private static final NullWritable valueOut = NullWritable.get();

        // For each node, emit a key-value pair Node, Null, in order to exploit the shuffle and sort phase of the reduces
        @Override
        public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
            node.setFromJson(value.toString());
            keyOut.setTitle(node.getTitle());
            keyOut.setPageRank(node.getPageRank());
            context.write(keyOut, valueOut);
        }
    }


    public static class SortReducer extends Reducer<Node, NullWritable, Text, DoubleWritable> {
        private static final Text keyOut = new Text();
        private static final DoubleWritable valueOut = new DoubleWritable();

        // Emit the already sorted list of pages (exploits of Shuffle & Sort phase)
        @Override
        public void reduce(final Node key, final Iterable<NullWritable> values, final Context context) throws IOException, InterruptedException {
            keyOut.set(key.getTitle());
            valueOut.set(key.getPageRank());
            context.write(keyOut, valueOut);
        }
    }


    public boolean run() throws Exception {

        // set configuration
        final Configuration conf = new Configuration();

        // instantiate job
        final Job job = new Job(conf, "Sort");
        job.setJarByClass(Sort.class);

        // set mapper/reducer
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Node.class);
        job.setMapOutputValueClass(NullWritable.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // define I/O
        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(this.output));

        // define input/output format
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }
}
