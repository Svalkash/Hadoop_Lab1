import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import shlabs.lab1.L1Mapper;
import shlabs.lab1.L1Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<IntWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<IntWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        L1Mapper mapper = new L1Mapper();
        L1Reducer reducer = new L1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        Configuration confM = mapDriver.getConfiguration();
        String metricIDsStr = "1,metricName,2,secondName";
        confM.setStrings("metricIDs", metricIDsStr);
        String scaleStr = "1m";
        confM.setStrings("scale", scaleStr);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        Configuration confR = reduceDriver.getConfiguration();
        String functionStr = "avg";
        confR.setStrings("function", functionStr);
        confR.setStrings("scale", scaleStr);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        Configuration confMR = mapReduceDriver.getConfiguration();
        confMR.setStrings("metricIDs", metricIDsStr);
        confMR.setStrings("scale", scaleStr);
        confMR.setStrings("function", functionStr);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new IntWritable(), new Text("1, 76123, 10"))
                .withOutput(new Text("metricName, 60000"), new IntWritable(10))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(10));
        values.add(new IntWritable(15));
        values.add(new IntWritable(20));
        reduceDriver
                .withInput(new Text("metricName, 60000"), values)
                .withOutput(new Text("metricName, 60000, 1m"), new IntWritable(15))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new IntWritable(), new Text("1, 145947, 30"))
                .withInput(new IntWritable(), new Text("1, 74123, 40"))
                .withInput(new IntWritable(), new Text("2, 12345, 5"))
                .withInput(new IntWritable(), new Text("2, 23456, 15"))
                .withOutput(new Text("metricName, 120000, 1m"), new IntWritable(30))
                .withOutput(new Text("metricName, 60000, 1m"), new IntWritable(40))
                .withOutput(new Text("secondName, 0, 1m"), new IntWritable(10))
                .runTest();
    }
}
