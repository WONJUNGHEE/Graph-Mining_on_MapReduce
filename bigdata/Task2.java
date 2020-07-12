package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Task2 extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Task2(),args);
    }

    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        Job job = Job.getInstance(getConf());
        job.setJarByClass(Task2.class);

        job.setMapperClass(TSlMap.class);
        job.setReducerClass(TSlReduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));


        job.waitForCompletion(true);
        return 0;
    }

    public static class TSlMap extends Mapper<Object, Text, IntWritable, IntWritable>{

        IntWritable ou = new IntWritable();
        IntWritable ov = new IntWritable();
        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());

            ou.set(Integer.parseInt(st.nextToken()));
            ov.set(Integer.parseInt(st.nextToken()));
            context.write(ov,ou);
            context.write(ou,ov);

        }
    }
    public static class TSlReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        IntWritable ov = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
                              Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            List<Integer> neighbors = new ArrayList<>();
            for(IntWritable v : values) {
                neighbors.add(v.get());
            }
            ov.set(neighbors.size());
            context.write(key, ov);
        }
    }
}