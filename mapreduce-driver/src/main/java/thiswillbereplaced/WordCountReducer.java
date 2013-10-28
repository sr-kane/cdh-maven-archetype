package thiswillbereplaced;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: skane
 * Date: 10/26/13
 * Time: 10:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordCountReducer extends
        org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>{

    private final IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);    //To change body of overridden methods use File | Settings | File Templates.
    }
}
