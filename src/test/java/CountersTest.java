
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

/**
 * Тесты функционирования счётчиков для ошибочных ситуаций для маппера.
 */
@Log4j
public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    /**
     * Начальная настройка, подготовка параметров конфигурации.
     */
    @Before
    public void setUp() {
        L1Mapper mapper = new L1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        Configuration confM = mapDriver.getConfiguration();
        String metricIDsStr = "1,metricName,2,secondName"; // Тестовая строка с расшифровкой metricID
        confM.setStrings("metricIDs", metricIDsStr);
        String scaleStr = "1s"; // Масштаб - 1 секунда
        confM.setStrings("scale", scaleStr);
    }

    /**
     * Проверка счётчика MALFORMED
     */
    @Test
    public void testMapperCounterMalformed() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(",,,,,")) // некорректный ввод
                .withInput(new LongWritable(), new Text("1234")) // некорректный ввод
                .withInput(new LongWritable(), new Text("sus, sus, sus")) // некорректный ввод
                .runTest();

        assertEquals("Expected 3 MALFORMED increment", 3,
                mapDriver.getCounters().findCounter(CounterType.MALFORMED).getValue());
        assertEquals("Expected no METRICNF increment", 0,
                mapDriver.getCounters().findCounter(CounterType.METRICNF).getValue());
    }

    /**
     * Проверка счётчика METRICNF
     */
    @Test
    public void testMapperCounterNotFound() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text("3, 123, 54")) // несуществующий metricID
                .runTest();

        assertEquals("Expected no MALFORMED increment", 0,
                mapDriver.getCounters().findCounter(CounterType.MALFORMED).getValue());
        assertEquals("Expected 1 METRICNF increment", 1,
                mapDriver.getCounters().findCounter(CounterType.METRICNF).getValue());
    }

    /**
     * Проверка, что счётчики равны 0 при получении верных данных
     */
    @Test
    public void testMapperCounterNone() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text("1, 2022, 50")) // корректная строка
                .withOutput(new Text("metricName, 2000"), new IntWritable(50))
                .runTest();

        assertEquals("Expected no MALFORMED increment", 0,
                mapDriver.getCounters().findCounter(CounterType.MALFORMED).getValue());
        assertEquals("Expected no METRICNF increment", 0,
                mapDriver.getCounters().findCounter(CounterType.METRICNF).getValue());
    }

    /**
     * Проверка, одновременного получения верных данных, некорректной строки и несуществующего metricID
     */
    @Test
    public void testMapperCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text("sus, sus, sus")) // некорректный ввод
                .withInput(new LongWritable(), new Text("3, 123, 54")) // несуществующий metricID
                .withInput(new LongWritable(), new Text("1, 2022, 50")) // корректная строка
                .withOutput(new Text("metricName, 2000"), new IntWritable(50))
                .runTest();

        assertEquals("Expected 1 MALFORMED increment", 1,
                mapDriver.getCounters().findCounter(CounterType.MALFORMED).getValue());
        assertEquals("Expected 1 METRICNF increment", 1,
                mapDriver.getCounters().findCounter(CounterType.METRICNF).getValue());
    }
}

