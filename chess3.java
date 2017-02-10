import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.text.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

//import org.apache.hadoop.mapreduce;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.nio.Buffer;
import java.nio.ByteBuffer;


public class chess3 
{

    
    public final static String strStep="\\[PlyCount \"[0-9]*\"\\]";
    private static int stepCounts = 0;

    public static class REMapper
            extends Mapper<Object, Text, Text, IntWritable> 
            {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException 
        {

            String str = value.toString();
            String temp = null;
            
            Pattern patStep=Pattern.compile(strStep);

            Matcher macStep = patStep.matcher(str);
           
            
            if (macStep .find()) 
            {
                temp = macStep .group(0);
                temp = temp.replaceAll("\\[PlyCount \"", "");
                temp = temp.replaceAll("\"]", "");
                word.set(temp);

                context.write(word,one );
               // stepCounts++;
                context.getCounter(MyCounters.counter).increment(1);
            }
            
        }
    }
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException 
        {
            int sum = 0;
            for (IntWritable val : values) 
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static class IntStepSumReducer
            extends Reducer<Text, IntWritable, Text, Text> {
        DecimalFormat df = new DecimalFormat("#.####");
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            String s = context.getConfiguration().get("sum");
            int count= Integer.parseInt(s);

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            //keySet.toArray();
            String avg=df.format((double)sum *100/ count);
            context.write(key, new Text(avg+"%"));
        }
    }


    public class IntKeyDescComparator extends WritableComparator {

    protected IntKeyDescComparator() {
        super(IntWritable.class, true);

    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -super.compare(a, b);
    }

    }



        static enum MyCounters{ counter }

   


    public static void main(String[] args) throws Exception {
       

        Configuration conf0 = new Configuration();
        Job job0 = Job.getInstance(conf0, "result0 count");
        job0.setJarByClass(chess3.class);
        job0.setMapperClass(REMapper.class);
        job0.setReducerClass(IntSumReducer.class);
        job0.setMapOutputKeyClass(Text.class);
        job0.setMapOutputValueClass(IntWritable.class);
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(Text.class);
       
        FileInputFormat.addInputPath(job0, new Path(args[0]));
        FileOutputFormat.setOutputPath(job0, new Path("temp2"));

        job0.waitForCompletion(true); 
        Counter counter = job0.getCounters().findCounter(MyCounters.counter);
        long tCount = counter.getValue();
        String sCount = String .valueOf(tCount);

        Configuration conf = new Configuration();
        conf.set("sum",sCount);

        Job job = Job.getInstance(conf, "result count");
        job.setJarByClass(chess3.class);
        job.setMapperClass(REMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntStepSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setSortComparatorClass(IntKeyDescComparator.class);
        //job.setSortComparatorClass(IntKeyAscComparator.class);
       // job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
