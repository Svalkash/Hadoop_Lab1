package shlabs.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Log4j
//public class L1Mapper extends Mapper<LongWritable, Text, Pair<Text, LongWritable>, IntWritable> {
public class L1Mapper extends Mapper<IntWritable, Text, Text, IntWritable> {

    //static Map<Integer, Text> metricIDs = new HashMap<>();
    Map<Integer, String> metricIDs;
    String scaleStr;
    long scale;

    protected Map<Integer, String> metricStringToMap(String[] strArr) {
        Map<Integer, String> ret = new HashMap<>();
        for (int i = 0; i < strArr.length; i += 2)
            ret.put(Integer.parseInt(strArr[i]), strArr[i + 1]);
        return ret;
    }

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        metricIDs = metricStringToMap(conf.getStrings("metricIDs")); //get metrics
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
        context.write(new Text(metricIDs.get((int)result[0]) + ", " + result[1] / scale * scale), new IntWritable((int)result[2]));
    }
}
