import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Created by milletpu on 2017/4/14.
 * E-mail: pujun@cnic.cn
 */

public class Hw2Part1 {

    // sb4tF0D0 yH12ZA30gq 296.289
    // oHuCS oHuCS 333.962
    // ouput this: <key = "source destination", value = double>

    public static class SourceMapper extends Mapper<Object, Text, Text, Text>{

        private Text source_dest = new Text();
        private Text one_duration = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Get the [source dest]
            String[] eachLine = value.toString().split(" ");
            source_dest.set(eachLine[0] + " " + eachLine[1]);
            one_duration.set("1" + " " + eachLine[2]);
            context.write(source_dest, one_duration);
        }
    }

    // sb4tF0D0 yH12ZA30gq 1 296.289
    // oHuCS oHuCS 1 333.962
    // sb4tF0D0 yH12ZA30gq 1 296.289
    // output this: sb4tF0D0 yH12ZA30gq 2 296.289
    public static class SumCombiner extends Reducer<Text, Text, Text, Text> {
        private Text count_duration= new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            double duration = 0;
            String[] temp;
            for (Text val : values) {
                temp = val.toString().split(" ");
                duration = duration + Double.parseDouble(temp[1]);
                sum = sum + Integer.parseInt(temp[0]);
            }
            count_duration.set(sum + " " + duration);
            context.write(key, count_duration);
        }
    }

    // sb4tF0D0 yH12ZA30gq 1 296.289
    // oHuCS oHuCS 1 333.962
    // sb4tF0D0 yH12ZA30gq 1 296.289
    // output this: sb4tF0D0 yH12ZA30gq 2 296.289
    public static class SumReducer extends Reducer<Text, Text, Text, Text> {
        private Text count_duration= new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            double duration = 0;
            String[] temp;
            for (Text val : values) {
                temp = val.toString().split(" ");
                duration = duration + Double.parseDouble(temp[1]);
                sum = sum + Integer.parseInt(temp[0]);
            }
            double avgDuration = duration/sum;
            BigDecimal res = new BigDecimal(avgDuration);
            avgDuration = res.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
            count_duration.set(sum + " " + avgDuration);
            context.write(key, count_duration);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Hw2Part1 <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Hw2Part1");

        job.setJarByClass(Hw2Part1.class);

        job.setMapperClass(SourceMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // add the input paths as given by command line
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        // add the output path as given by the command line
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
