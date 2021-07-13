package it.unipi.hadoop;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class Count {
    private static final String PATH = "/count";
    private String input;
    private String output;
    
    public Count(final String input, final String output){
        this.input = input;
        this.output = output;
    }

    public static class CountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        private static final Text key = new Text("Pages");
        private static final IntWritable value = new IntWritable();

    // in-mapping combining    
	private static int count;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            count = 0;
        }

	@Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
	        count++;
        }

	@Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            value.set(count);
            context.write(key,value);
        }

    }


    public static class CountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {
        private static final IntWritable result = new IntWritable();
        private static int sum;
        @Override
        public void reduce(final Text key, final Iterable<IntWritable>values, final Context context) throws IOException, InterruptedException{
            sum = 0;
            for (final IntWritable i : values) {
                sum += i.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
    public boolean run() throws Exception {
        // set configurations
        final Configuration conf = new Configuration();
        // instantiate job
        final Job job = new Job(conf, "Count");
        job.setJarByClass(Count.class);
        // set mapper/combiner/reducer
        job.setMapperClass(CountMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);
        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // define I/O
        FileInputFormat.addInputPath(job, new Path(this.input));
        FileOutputFormat.setOutputPath(job, new Path(this.output + PATH));
        // define input/output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        return job.waitForCompletion(true);
    }
    
    public int getTotalPages() throws Exception {
        // read result
        final String file = output + PATH + "/part-r-00000";
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Path hdfsReadPath = new Path(file);
        FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        String line = bufferedReader.readLine();

        bufferedReader.close();
        inputStream.close();
        fileSystem.close();

        String[] tokens = line.trim().split("\t");

        int pageCount = Integer.parseInt(tokens[1]);

        return pageCount;
    }

    public static void main(String[] args){

    }
}
