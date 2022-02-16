
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import shlabs.lab1.CounterType;
import shlabs.lab1.L1Mapper;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@Log4j
public class CountersTest {

    private MapDriver<IntWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void setUp() {
        L1Mapper mapper = new L1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        Configuration confM = mapDriver.getConfiguration();
        String metricIDsStr = "1,metricName,2,secondName";
        confM.setStrings("metricIDs", metricIDsStr);
        String scaleStr = "1s";
        confM.setStrings("scale", scaleStr);
    }

    @Test
    public void testMapperCounterMalformed() throws IOException  {
        mapDriver
                .withInput(new IntWritable(), new Text(",,,,,"))
                .withInput(new IntWritable(), new Text("1234"))
                .withInput(new IntWritable(), new Text("sus, sus, sus"))
                .runTest();

        assertEquals("Expected 3 MALFORMED increment", 3,
                mapDriver.getCounters().findCounter(CounterType.MALFORMED).getValue());
        assertEquals("Expected no METRICNF increment", 0,
                mapDriver.getCounters().findCounter(CounterType.METRICNF).getValue());
    }

    @Test
    public void testMapperCounterNotFound() throws IOException  {
        mapDriver
                .withInput(new IntWritable(), new Text("3, 123, 54"))
                .runTest();

        assertEquals("Expected no MALFORMED increment", 0,
                mapDriver.getCounters().findCounter(CounterType.MALFORMED).getValue());
        assertEquals("Expected 1 METRICNF increment", 1,
                mapDriver.getCounters().findCounter(CounterType.METRICNF).getValue());
    }

    @Test
    public void testMapperCounterNone() throws IOException  {
        mapDriver
                .withInput(new IntWritable(), new Text("1, 2022, 50"))
                .withOutput(new Text("metricName, 2000"), new IntWritable(50))
                .runTest();

        assertEquals("Expected no MALFORMED increment", 0,
                mapDriver.getCounters().findCounter(CounterType.MALFORMED).getValue());
        assertEquals("Expected no METRICNF increment", 0,
                mapDriver.getCounters().findCounter(CounterType.METRICNF).getValue());
    }

    @Test
    public void testMapperCounters() throws IOException {
        mapDriver
                .withInput(new IntWritable(), new Text("sus, sus, sus"))
                .withInput(new IntWritable(), new Text("3, 123, 54"))
                .withInput(new IntWritable(), new Text("1, 2022, 50"))
                .withOutput(new Text("metricName, 2000"), new IntWritable(50))
                .runTest();

        assertEquals("Expected 1 MALFORMED increment", 1,
                mapDriver.getCounters().findCounter(CounterType.MALFORMED).getValue());
        assertEquals("Expected 1 METRICNF increment", 1,
                mapDriver.getCounters().findCounter(CounterType.METRICNF).getValue());
    }
}

