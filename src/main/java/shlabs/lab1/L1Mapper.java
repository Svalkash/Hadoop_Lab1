package shlabs.lab1;

import com.opencsv.CSVReader;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Log4j
public class L1Mapper extends Mapper<LongWritable, Text, Pair<Integer, Long>, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    static Map<Integer, String> metrics = new HashMap<>();

    public static void readMetrics(String filename) {
        //"./src/main/resources/yourfile.csv"
        CSVReader reader = null;
        try {
            reader = new CSVReader(new FileReader(filename));
            //reader = new CSVReader(new FileReader("yourfile.csv"));
        } catch (Exception ex) {
            log.fatal("Csv loading error: " + ex.getMessage());
            System.exit(1);
        }

        String[] nextLine;
        // nextLine[] is an array of values from the line
        try{
            while ((nextLine = reader.readNext()) != null) {
                if (nextLine.length > 2) {
                    log.fatal("Csv loading error: too long line");
                    System.exit(1);
                }
                metrics.put(Integer.parseInt(nextLine[0]), nextLine[1]); // output = 25
            }
        }
        catch (Exception ex){
            log.fatal("Exception while trying to read metrics config: " + ex.getMessage());
            System.exit(1);
        }
        metrics.forEach((k, v) -> log.info("Decoded data: " + k + " = " + v));
    }

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        readMetrics(conf.getStrings("metricsFile")[0]); //get metrics data from file
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        long[] result;
        try {
            result = Arrays.stream(value.toString().split(", ")).mapToLong(Long::parseLong).toArray();
        }
        catch (Exception ex) {
            log.error(ex.getMessage());
            context.getCounter(CounterType.MALFORMED).increment(1);
            return;
        }
        if (result.length != 3) {
            context.getCounter(CounterType.MALFORMED).increment(1);
            return;
        }
        if (!metrics.containsKey(result[0])) {
            context.getCounter(CounterType.METRICNF).increment(1);
            return;
        }
        //now that

        String line = value.toString();
        UserAgent userAgent = UserAgent.parseUserAgentString(line);
        if (userAgent.getBrowser() == Browser.UNKNOWN) {
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            word.set(userAgent.getBrowser().getName());
            context.write(word, one);
        }
    }
}
