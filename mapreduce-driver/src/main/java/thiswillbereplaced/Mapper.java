package thiswillbereplaced;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;

public class Mapper extends
        org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);

    protected void map( LongWritable offset, Text value, Context context)
            throws IOException, InterruptedException {

        StringTokenizer tok = new StringTokenizer( value.toString());

        while ( tok.hasMoreTokens()) {
            Text word = new Text( tok.nextToken());
            context.write( word, one);
        }
    }

    public Mapper() {
    }
}
