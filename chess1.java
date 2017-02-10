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
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob.State;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
//import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;


public class chess1 {
  
    public final static String strRes = "\\[Result \"[^a-z]*\"]";
    
    

    public static class REMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        // static enum MyCounters{
        // counter

        // }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private final String whtWin = "1-0";
        private final String blkWin = "0-1";
        private final String draw = "1/2-1/2";
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException 
        {

            String str = value.toString();
            String temp = null;
            Pattern patRes=Pattern.compile(strRes);
            Matcher macRes= patRes.matcher(str);
            if (macRes.find()) 
            {
                temp = macRes.group(0);
                temp = temp.replaceAll("\\[Result \"", "");
                temp=temp.replaceAll("\"]", "");
                switch (temp) 
                {
                case whtWin:
                    temp="White";
                    break;
                case blkWin:
                    temp="Black";
                    break;
                case draw:
                    temp="Draw";
                    break;
                default:
                    break;
                }
                word.set(temp);
                context.write(word, one);
                context.getCounter(MyCounters.counter).increment(1);
            }

        }
    }

    public static class Combiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) 
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, Text> {

        DecimalFormat df = new DecimalFormat("#.##");

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            ///////////////////////
           // double count  = context.getCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
            String s = context.getConfiguration().get("sum");
            int count= Integer.parseInt(s);

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            String avg = df.format((double)sum / count);
            context.write(key, new Text(sum+" "+avg));
        }
    }

        static enum MyCounters{
        counter

        }
    ////////////////////////////////////////////
    public static void main(String[] args) throws Exception {

       //JobControl jc = new JobControl("GROUP_NAME"); 


        Configuration conf0 = new Configuration();
        Job job0 = Job.getInstance(conf0, "result0 count");
        job0.setJarByClass(chess1.class);
        job0.setMapperClass(REMapper.class);
        job0.setReducerClass(Combiner.class);
        job0.setMapOutputKeyClass(Text.class);
        job0.setMapOutputValueClass(IntWritable.class);
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(Text.class);
        //job0.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job0, new Path(args[0]));
        FileOutputFormat.setOutputPath(job0, new Path("stepTmp"));

        job0.waitForCompletion(true); 
        Counter counter = job0.getCounters().findCounter(MyCounters.counter);
        long tCount = counter.getValue();
        String sCount = String .valueOf(tCount);

        Configuration conf = new Configuration();
        conf.set("sum",sCount);

        Job job = Job.getInstance(conf, "result count");
        job.setJarByClass(chess1.class);
        job.setMapperClass(REMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //jobControl.set(job);
        //jc.addJob(job);  
  
        //job.addDependingJob(job0);  
        //jc.run();  
        //return jobControl.getFailedJobs() == null || jobControl.getFailedJobs().isEmpty() ? 0 : 1;
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
