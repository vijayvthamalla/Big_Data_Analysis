import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import javax.print.attribute.standard.JobName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.checkerframework.checker.units.qual.Length;


class Vertex implements Writable {
    public short tag;           // 0 for a graph vertex, 1 for a group number
    public long group;          // the group where this vertex belongs to
    public long VID;            // the vertex ID
    // public long[] adjacent;     // the vertex neighbors
    public Vector<Long> adjacent;
    /* ... */
    Vertex(){}

    Vertex(short tag,long group,long VID,Vector<Long> adjacent){
        this.tag = tag;
        this.group = group;
        this.VID = VID;
        this.adjacent = adjacent;
    }
    Vertex(short tag,long group){
        this.tag = tag;
        this.group = group;
        this.VID = 0;
        this.adjacent = new Vector<Long>();
    }
    public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        out.writeLong(group);
        out.writeLong(VID);
        out.writeInt(adjacent.size());
        for (int i = 0; i < adjacent.size(); i++) {
            out.writeLong(adjacent.get(i));
        }
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        group = in.readLong();
        VID = in.readLong();
        int lenAdj = in.readInt();
        adjacent = new Vector<Long>(lenAdj);
        for (int i = 0; i < lenAdj; i++) {
            adjacent.add(in.readLong());
        }
    }
    @Override
    public String toString () {
        String s="\n";
        for ( int i = 0; i < adjacent.size(); i++ ) {
        s += Long.toString(adjacent.get(i));
        }
        return ""+tag+"\t"+group+" \t"+VID+"\t"+s;
    }
}

public class Graph {

    /* ... */
    public static class Mapper1 extends Mapper<Object,Text,LongWritable, Vertex> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
        String str[] = value.toString().split(",");
        long vID = Long.parseLong(str[0]);
        Vector<Long> adjacent = new Vector<Long>();
        for (int i=1; i<str.length; i++){
            adjacent.add(Long.parseLong(str[i]));
        }
        context.write(new LongWritable(vID), new Vertex((short)0, vID, vID,adjacent));
        // System.out.println(adjacent);
        }
    }

    public static class Mapper2 extends Mapper<LongWritable,Vertex,LongWritable, Vertex> {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
            context.write(new LongWritable(value.VID),value);
            for (long n:value.adjacent){
                context.write(new LongWritable(n),new Vertex((short) 1,value.group));
            }
        }
    }

    public static class Reducer2 extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                           throws IOException, InterruptedException {
            long m = Long.MAX_VALUE;
            Vector<Long> adj = new Vector<Long>();
            for (Vertex v:values){
                if(v.tag==0){
                    adj = v.adjacent;
                }
                m = Math.min(m,v.group);
            }
            context.write(new LongWritable(m),new Vertex((short) 0,m,key.get(),adj));
        }
    }

    public static class Mapper3 extends Mapper<LongWritable,Vertex,LongWritable, IntWritable> {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
            context.write(key,new IntWritable(1));
        }
    }

    public static class Reducer3 extends Reducer<LongWritable,IntWritable,LongWritable,IntWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            int m = 0;
            for (IntWritable v: values){
                m += v.get();
            }
            context.write(key,new IntWritable(m));
        }
    }

    public static void main ( String[] args ) throws Exception {
        
        /* ... First Map-Reduce job to read the graph */
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1);
        job1.setJobName("Graph");

        job1.setJarByClass(Graph.class);
        job1.setMapperClass(Mapper1.class);
        job1.setNumReduceTasks(0);

        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);

        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/f0"));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.waitForCompletion(true);

        /* ... Second Map-Reduce job to propagate the group number */
        for ( short i = 0; i < 5; i++ ) {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2);
            job2.setJobName("Graph");
			job2.setJarByClass(Graph.class);
            job2.setMapperClass(Mapper2.class);
			job2.setReducerClass(Reducer2.class);

            job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Vertex.class);
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Vertex.class);

			FileInputFormat.setInputPaths(job2,new Path(args[1]+"/f"+i));
			FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/f"+(i+1)));
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            job2.waitForCompletion(true);
        }
        
        /* ... Final Map-Reduce job to calculate the connected component sizes */
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3);
        job3.setJobName("Graph");
		job3.setJarByClass(Graph.class);
		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(IntWritable.class);                        		       		

		job3.setMapperClass(Mapper3.class);
		job3.setReducerClass(Reducer3.class);

		FileInputFormat.setInputPaths(job3,new Path(args[1]+"/f5"));
		FileOutputFormat.setOutputPath(job3,new Path(args[2]));
		job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        job3.waitForCompletion(true);
    }
}

