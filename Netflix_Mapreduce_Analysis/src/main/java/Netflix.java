import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Netflix {
    public static class MyMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            if (!value.toString().contains(":")){
		        int user = s.nextInt();
            	int rating = s.nextInt();
            	context.write(new IntWritable(user),new IntWritable(rating));
		}
            s.close();
        }
    }

    public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            double sum = 0.0;
            long count = 0;
            for (IntWritable v: values) {
                sum += v.get();
                count++;
            };
            context.write(key,new IntWritable((int) (sum/count*10) ));
        }
    }

    public static class MyMapper2 extends Mapper<Object,Text,DoubleWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\t");
            int user = s.nextInt();
            int rating = s.nextInt();
            context.write(new DoubleWritable(rating),new IntWritable(1));
            s.close();
        }
    }

    public static class MyReducer2 extends Reducer<DoubleWritable,IntWritable,DoubleWritable,IntWritable> {
        @Override
        public void reduce ( DoubleWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            double rating = key.get();
            double sum = 0.0;
            for (IntWritable v: values) {
                sum += v.get();
            };
            context.write(new DoubleWritable(rating/10.0),new IntWritable( (int) sum));
        }
    }

    public static void main ( String[] args ) throws Exception {

        /*Job 1 */
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf,"First Job");

        job1.setJarByClass(Netflix.class);
        job1.setMapperClass(MyMapper.class);
        job1.setReducerClass(MyReducer.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]));
        job1.waitForCompletion(true);

        /*Job 2 */
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2,"Second Job");

        job2.setJarByClass(Netflix.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);

        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0:1);
    }
}

