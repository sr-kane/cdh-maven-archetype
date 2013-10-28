package thiswillbereplaced;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WordCountDriverTest {

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp(){
        Mapper mapper = new Mapper();
        WordCountReducer wordCountReducer = new WordCountReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(wordCountReducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, wordCountReducer);
    }

    @Test
    public void testMapper() throws Exception {
        // Set Config values
        // Configuration config = mapDriver.getConfiguration();
        // config.set("","");

        mapDriver.withInput(new LongWritable(), new Text("word1 word2 word1"));

        mapDriver.withOutput(new Text("word1"), new IntWritable(1));
        mapDriver.withOutput(new Text("word2"), new IntWritable(1));
        mapDriver.withOutput(new Text("word1"), new IntWritable(1));

        mapDriver.runTest();

        // Test Counters
        // assertEquals("",1,mapDriver.getCounters().findCounter(ENUM.COUNTER1).getValue());
    }

    @Test
    public void testReducer() throws Exception {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("word1"), values);
        reduceDriver.withOutput(new Text("word1"), new IntWritable(2));
        reduceDriver.runTest();
    }
}
