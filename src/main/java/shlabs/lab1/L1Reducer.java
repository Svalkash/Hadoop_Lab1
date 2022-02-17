package shlabs.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Редьюсер: суммирует все единицы полученные от {@link L1Mapper}, выдаёт суммарное количество пользователей по браузерам
 */
@Log4j
public class L1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    String scaleStr;
    String funStr;
    static final String[] funNames = { "avg", "max", "min" };

    @Override
    protected void setup(Context context) {
        scaleStr = context.getConfiguration().getStrings("scale")[0]; //get scale from file
        funStr = context.getConfiguration().getStrings("function")[0]; //get function from file
        if (!ArrayUtils.contains(funNames, funStr)) {
            log.fatal("Wrong function name provided");
            System.exit(1);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int result = 0;
        int cnt = 0;
        int tmp;
        //iterate
        for (IntWritable value : values) {
            tmp = value.get();
            switch (funStr) {
                case "max":
                    if (tmp > result)
                        result = tmp;
                    break;
                case "min":
                    if (tmp < result)
                        result = tmp;
                    break;
                case "avg":
                default:
                    result += tmp;
                    ++cnt;
                    break;
            }
        }
        //finalize
        switch(funStr) {
            case "max":
            case "min":
                break;
            case "avg":
            default:
                result /= cnt;
                break;
        }
        context.write(new Text(key.toString() + ", " + scaleStr), new IntWritable(result));
    }
}
