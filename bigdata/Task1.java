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

public class Task1 extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Task1(),args);
    }

    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        Job job = Job.getInstance(getConf());
        job.setJarByClass(Task1.class);

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
            if(ou.get()<ov.get()) context.write(ou,ov);
            else if(ou.get()>ov.get()) context.write(ov, ou);

        }
    }
    public static class TSlReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

        IntWritable ou = new IntWritable();
        IntWritable ov = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
                              Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            List<Integer> neighbors = new ArrayList<>();
            for(IntWritable v : values) {
                if(neighbors.contains(v.get())) {
                    continue;
                }else {
                    neighbors.add(v.get());
                }
            }
            for(int i=neighbors.size();i>0;i--) {
                ou.set(neighbors.get(i-1));
                context.write(key, ou);
            }
        }
    }
}
