import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class MRB {

    private static Map<String, Integer> gameTimes = new HashMap<String, Integer>();

    private static IntWritable[] win = {new IntWritable(1), new IntWritable(0), new IntWritable(0)};
    private static IntWritable[] lose = {new IntWritable(0), new IntWritable(1), new IntWritable(0)};
    private static IntWritable[] draw = {new IntWritable(0), new IntWritable(0), new IntWritable(1)};

    public static class REMapper
            extends Mapper<Object, Text, Text, IntArrayWritable> {

        private final static IntArrayWritable one = new IntArrayWritable(IntWritable.class, win);  // mark win
        private final static IntArrayWritable two = new IntArrayWritable(IntWritable.class, lose);  // mark lose
        private final static IntArrayWritable three = new IntArrayWritable(IntWritable.class, draw);// mark draw

        private final static String strWhite = "\\[White \"[A-Za-z ]*\"\\]";
        private final static String strBlack = "\\[Black \"[A-Za-z ]*\"\\]";
        private final static String strResult = "\\[Result \"[^a-z]*\"]";
        private final static Pattern whitePn = Pattern.compile(strWhite);
        private final static Pattern blackPn = Pattern.compile(strBlack);
        private final static Pattern resultPn = Pattern.compile(strResult);

        private final String strWhiteWin = "1-0";
        private final String strBlackWin = "0-1";
        private final String strDraw = "1/2-1/2";

        // id + " " + white/black
        String whiteKey = "";
        String blackKey = "";

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String txt = value.toString();
            Matcher whiteMatcher = whitePn.matcher(txt);
            Matcher blackMatcher = blackPn.matcher(txt);
            Matcher resultMatcher = resultPn.matcher(txt);

            if (whiteMatcher.find()) {
                String temp = whiteMatcher.group(0);
                temp = temp.replaceAll("\\[White \"", "").replaceAll("\"]", "");
                whiteKey = temp + " white";
            } else if (blackMatcher.find()) {
                String temp = blackMatcher.group(0);
                temp = temp.replaceAll("\\[Black \"", "").replaceAll("\"]", "");
                blackKey = temp + " black";
            } else if (resultMatcher.find()) {
                String temp = resultMatcher.group(0);
                temp = temp.replaceAll("\\[Result \"", "").replaceAll("\"]", "");
                switch (temp) {
                    case strWhiteWin:
                        context.write(new Text(whiteKey), one);
                        context.write(new Text(blackKey), two);
                        break;
                    case strBlackWin:
                        context.write(new Text(whiteKey), two);
                        context.write(new Text(blackKey), one);
                        break;
                    case strDraw:
                        context.write(new Text(whiteKey), three);
                        context.write(new Text(blackKey), three);
                        break;
                    default:
                        break;
                }
                increaseTimesByOne(whiteKey);
                increaseTimesByOne(blackKey);
            }
        }
    }

    public static class ResultCombiner
            extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {

        public void reduce(Text key, Iterable<ArrayWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            Integer[] record = {0, 0, 0};
            for (ArrayWritable val : values) {
                IntArrayWritable iaw = (IntArrayWritable) val;
                if ("1_0_0_".equals(iaw)) {
                    record[0]++;
                } else if ("0_1_0_".equals(iaw)) {
                    record[1]++;
                } else if ("0_0_1_".equals(iaw)) {
                    record[2]++;
                }
            }
            IntWritable[] result = {new IntWritable(record[0]), new IntWritable(record[1]),
                                    new IntWritable(record[2])};
            context.write(key, new IntArrayWritable(IntWritable.class, result));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntArrayWritable, Text, Text> {

        DecimalFormat df = new DecimalFormat("#.##");

        public void reduce(Text key, Iterable<IntArrayWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int[] sum = {0, 0, 0};

            for (IntArrayWritable value : values) {
                Writable[] val = value.get();
                int i=0;
                for (Writable writable : val) {
                    IntWritable intWritable = (IntWritable) writable;
                    sum[i] += intWritable.get();
                    i++;
                }
            }
            int times = gameTimes.get(key.toString());
            System.out.println(times+" "+sum[0]+" "+sum[1]+" "+sum[2]);
            String result = df.format((double)sum[0]/times)+" "+
                    df.format((double)sum[1]/times)+" "+
                    df.format((double)sum[2]/times);
            context.write(key, new Text(result));
        }
    }

    /**
     * increase the game times of key by one
     *
     * @param key formatted as id white/black
     */
    private static void increaseTimesByOne(String key) {
        if (!gameTimes.containsKey(key)) {
            gameTimes.put(key, 0);
        }
        int times = gameTimes.get(key);
        gameTimes.put(key, times + 1);
    }

    public static class IntArrayWritable extends ArrayWritable {

        public IntArrayWritable() {
             super(IntWritable.class);
        }

        public IntArrayWritable(Class<? extends Writable> valueClass) {
            super(valueClass);
        }

        public IntArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
            super(valueClass, values);
        }

        public IntArrayWritable(String[] strings) {
            super(strings);
        }

        @Override
        public Writable[] get() {
            return super.get();
        }

        @Override
        public String toString() {
            Writable[] writables = super.get();
            StringBuffer sb = new StringBuffer();
            for (Writable w : writables) {
                IntWritable i = (IntWritable) w;
                sb.append(i.get()+"_");
            }
            return sb.toString();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "result count");
        job.setJarByClass(MRB.class);
        job.setMapperClass(REMapper.class);
        job.setCombinerClass(ResultCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntArrayWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
