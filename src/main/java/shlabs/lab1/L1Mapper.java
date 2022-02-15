package shlabs.lab1;

import com.opencsv.CSVReader;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Log4j
//public class L1Mapper extends Mapper<LongWritable, Text, Pair<Text, LongWritable>, IntWritable> {
public class L1Mapper extends Mapper<IntWritable, Text, Text, IntWritable> {

    //static Map<Integer, Text> metricIDs = new HashMap<>();
    Map<Integer, String> metricIDs = new HashMap<>();
    String scaleStr;
    long scale;

    public void readmetricIDs(String filename) {
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
        try {
            while ((nextLine = reader.readNext()) != null) {
                if (nextLine.length > 2) {
                    log.fatal("Csv loading error: too long line");
                    System.exit(1);
                }
                //metricIDs.put(Integer.parseInt(nextLine[0]), new Text(nextLine[1]));
                metricIDs.put(Integer.parseInt(nextLine[0]), nextLine[1]);
            }
        } catch (Exception ex) {
            log.fatal("Exception while trying to read metricIDs config: " + ex.getMessage());
            System.exit(1);
        }
        metricIDs.forEach((k, v) -> log.info("Decoded data: " + k + " = " + v));
    }

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        readmetricIDs(conf.getStrings("metricIDsFile")[0]); //get metricIDs data from file
        scaleStr = conf.getStrings("scale")[0]; //get scale from file
        scale = scaleStr.equals("1s") ? 1000 :
                scaleStr.equals("10s") ? 10000 :
                        scaleStr.equals("1m") ? 60000 :
                                scaleStr.equals("10m") ? 60000 :
                                        scaleStr.equals("1h") ? 360000 :
                                                scaleStr.equals("1d") ? 8640000 :
                                                        0; //convert it to a number
        if (scale == 0) {
            log.fatal("Wrong scale value provided");
            System.exit(1);
        }
    }

    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        long[] result;
        Text keyOut = new Text();
        IntWritable valueOut = new IntWritable();

        try {
            result = Arrays.stream(value.toString().split(", ")).mapToLong(Long::parseLong).toArray();
        } catch (Exception ex) {
            log.error(ex.getMessage());
            context.getCounter(CounterType.MALFORMED).increment(1);
            return;
        }
        if (result.length != 3) {
            context.getCounter(CounterType.MALFORMED).increment(1);
            return;
        }
        if (!metricIDs.containsKey((int)result[0])) {
            context.getCounter(CounterType.METRICNF).increment(1);
            return;
        }
        //now we extracted all 3 parts, let's process them
        //context.write(new Pair(metricIDs.get(result[0]), ), one);
        keyOut.set(metricIDs.get((int)result[0]) + ", " + result[1] / scale);
        valueOut.set((int)result[2]);
        context.write(keyOut, valueOut);
    }
}
