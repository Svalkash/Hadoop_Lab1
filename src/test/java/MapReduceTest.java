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

/**
 * Тесты общей работоспособности классов маппера и редьюсера.
 */
public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    /**
     * Начальная настройка, подготовка параметров конфигурации.
     */
    @Before
    public void setUp() {
        String metricIDsStr = "1,metricName,2,secondName"; // Тестовая строка с расшифровкой metricID
        String scaleStr = "1m"; // Масштаб - 1 минута
        String functionStr = "avg"; // Функция - усреднение

        L1Mapper mapper = new L1Mapper();
        L1Reducer reducer = new L1Reducer();
        // Конфигурация драйвера маппера
        mapDriver = MapDriver.newMapDriver(mapper);
        Configuration confM = mapDriver.getConfiguration();
        confM.setStrings("metricIDs", metricIDsStr);
        confM.setStrings("scale", scaleStr);
        // Конфигурация драйвера редьюсера
        reduceDriver = ReduceDriver.newReduceDriver(reducer); 
        Configuration confR = reduceDriver.getConfiguration();
        confR.setStrings("function", functionStr);
        confR.setStrings("scale", scaleStr);
        // Конфигурация драйвера MapReduce
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        Configuration confMR = mapReduceDriver.getConfiguration(); 
        confMR.setStrings("metricIDs", metricIDsStr);
        confMR.setStrings("scale", scaleStr);
        confMR.setStrings("function", functionStr);
    }

    /**
     * Тест маппера - проверка расшифровки metricID и округления времени 
     */
    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text("1, 76123, 10"))
                .withOutput(new Text("metricName, 60000"), new IntWritable(10))
                .runTest();
    }

    /**
     * Тест редьюсера - проверка функции усреднения
     * Других функций в задании не было
     */
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

    /**
     * Тест одновременной работы маппера и редьюсера
     */
    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text("1, 145947, 30"))
                .withInput(new LongWritable(), new Text("1, 74123, 40"))
                .withInput(new LongWritable(), new Text("2, 12345, 5"))
                .withInput(new LongWritable(), new Text("2, 23456, 15"))
                .withOutput(new Text("metricName, 120000, 1m"), new IntWritable(30))
                .withOutput(new Text("metricName, 60000, 1m"), new IntWritable(40))
                .withOutput(new Text("secondName, 0, 1m"), new IntWritable(10))
                .runTest();
    }
}
