import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Triple implements Writable {
    public int i;
    public int j;
    public double value;
	
    Triple () {}
	
    Triple ( int i, int j, double v ) { this.i = i; this.j = j; value = v; }
	
    public void write ( DataOutput out ) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
        out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
        i = in.readInt();
        j = in.readInt();
        value = in.readDouble();
    }

    @Override
    public String toString () {
        return ""+i+"\t"+j+"\t"+value;
    }
}

class Block implements Writable {
    final static int rows = 100;
    final static int columns = 100;
    public double[][] data;

    Block () {
        data = new double[rows][columns];
    }

    public void write ( DataOutput out ) throws IOException {
        int row = data.length;
        int column = data[0].length;
        out.writeInt(row);
        out.writeInt(column);
        for (int i=0; i<row; i++){
            for (int j=0; j< column;j++){
                out.writeDouble(data[i][j]);
            }
        }
    }

    public void readFields ( DataInput in ) throws IOException {
        int row = in.readInt();
        int column = in.readInt();
        for (int i=0; i<row; i++){
            for (int j=0; j< column;j++){
                data[i][j] = in.readDouble();
            }
        }
    }

    @Override
    public String toString () {
        String s = "\n";
        for ( int i = 0; i < rows; i++ ) {
            for ( int j = 0; j < columns; j++ )
                s += String.format("\t%.3f",data[i][j]);
            s += "\n";
        }
        return s;
    }
}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;
	
    Pair () {}
	
    Pair ( int i, int j ) { this.i = i; this.j = j; }
	
    public void write ( DataOutput out ) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

    public void readFields ( DataInput in ) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }

    @Override
    public int compareTo ( Pair o ) {
        return (i == o.i) ? (int)(j-o.j) : (int)(i-o.i);
    }

    @Override
    public String toString () {
        return ""+i+"\t"+j;
    }
}

public class Add extends Configured implements Tool {
    final static int rows = Block.rows;
    final static int columns = Block.columns;

    public static class MyMapper1 extends Mapper<Object,Text,Pair,Triple> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i = s.nextInt();
            int j = s.nextInt();
            double v = s.nextDouble();
            context.write(new Pair(i/rows,j/columns),new Triple(i%rows,j%columns,v));
            s.close();
        }
    }

    public static class MyReducer1 extends Reducer<Pair,Triple,Pair,Block> {
        @Override
        public void reduce ( Pair key, Iterable<Triple> values, Context context )
                           throws IOException, InterruptedException {
            Block b = new Block();
            for (Triple val: values) {
                b.data[val.i][val.j] = val.value;
            }
            context.write(key,b);
        }
    }

    public static class MyMapper2 extends Mapper<Pair,Block,Pair,Block> {
        @Override
        public void map ( Pair key, Block value, Context context )
                        throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    public static class MyMapper3 extends Mapper<Pair,Block,Pair,Block> {
        @Override
        public void map ( Pair key, Block value, Context context )
                        throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    public static class MyReducer2 extends Reducer<Pair,Block,Pair,Block> {
        @Override
        public void reduce ( Pair key, Iterable<Block> values, Context context )
                           throws IOException, InterruptedException {
            Block s = new Block();
            for (Block b:values){
                for (int i=0;i<b.data.length;i++){
                    for (int j=0;j<b.data[0].length;j++){
                        s.data[i][j] += b.data[i][j];
                    }
                }
            }
            context.write(key,s);
        }
    }



    @Override
    public int run ( String[] args ) throws Exception {
        /* ... */
        return 0;
    }

    public static void main ( String[] args ) throws Exception {
    	ToolRunner.run(new Configuration(),new Add(),args);

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf,"First Job");

        job1.setJarByClass(Add.class);
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer1.class);

        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(Block.class);
        job1.setMapOutputKeyClass(Pair.class);
        job1.setMapOutputValueClass(Triple.class);
        
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2,"Second Job");

        job2.setJarByClass(Add.class);
        job2.setMapperClass(MyMapper1.class);
        job2.setReducerClass(MyReducer1.class);

        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(Block.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(Triple.class);
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);

        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3,"Third Job");

        job3.setJarByClass(Add.class);
        job3.setReducerClass(MyReducer2.class);

        job3.setOutputKeyClass(Pair.class);
        job3.setOutputValueClass(Block.class);
        
        job3.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job3,new Path(args[2]),SequenceFileInputFormat.class,MyMapper2.class);
        MultipleInputs.addInputPath(job3,new Path(args[3]),SequenceFileInputFormat.class,MyMapper3.class);
        FileOutputFormat.setOutputPath(job3,new Path(args[4]));
        job3.waitForCompletion(true);
    }
}

