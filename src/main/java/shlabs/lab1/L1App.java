package shlabs.lab1;

import com.opencsv.CSVReader;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.FileReader;
import java.util.*;


@Log4j
public class L1App {

    public static String readmetricIDs(String filename) {
        //"./src/main/resources/yourfile.csv"
        CSVReader reader;
        try {
            reader = new CSVReader(new FileReader(filename));
        } catch (Exception ex) {
            log.fatal("CSV loading error: " + ex.getMessage());
            return "";
        }

        try {
            List<String[]> stringList = reader.readAll();
            stringList.forEach((item)->{ if (item.length != 2) throw new RuntimeException("Wrong format"); }); //check format
            if (stringList.size() == 0)
                throw new RuntimeException("Empty CSV");
            Set<Integer> numSet = new LinkedHashSet<>(); // check for duplicates
            Set<String> nameSet = new LinkedHashSet<>();
            StringBuilder ret = new StringBuilder();
            String[] tmp;
            for (String[] strings : stringList) {
                if (!numSet.add(Integer.parseInt(strings[0])))
                    throw new RuntimeException("Duplicate metricIDs");
                if (!nameSet.add(strings[1]))
                    throw new RuntimeException("Duplicate metric names");
                ret.append(",").append(strings[0]).append(",").append(strings[1]);
            }
            return ret.substring(1);
        }
        catch(NumberFormatException ex) {
            log.error("metricID is not a number: " + ex.getMessage());
            return "";
        }
        catch(Exception ex) {
            log.error("Exception while trying to read metricIDs config: " + ex.getMessage());
            return "";
        }
    }

    public static void main(String[] args) throws Exception {
/*
        if (args.length < 5) {
            throw new RuntimeException("You should specify input and output folders, metcisIDs filename, scale and function!");
        }*/
        Configuration conf = new Configuration();
        String metricIDs = readmetricIDs(args[2]);
        //String metricIDs = readmetricIDs("./src/main/resources/metricNames.csv");
        if (Objects.equals(metricIDs, "")) {
            log.fatal("Couldn't read metricIDs, stopping.");
            return;
        }
        conf.setStrings("metricIDs", metricIDs);
        conf.setStrings("scale", args[3]);
        conf.setStrings("function", args[4]);

        //read metric IDs CSV and transform them to something nice


        Job job = Job.getInstance(conf, "Lab1Job");
        job.setJarByClass(L1App.class);
        job.setMapperClass(L1Mapper.class);
        job.setReducerClass(L1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class); //set output format to seq file
        //conf.set("mapreduce.output.textoutputformat.separator", ",");
        //job.setOutputFormatClass(TextOutputFormat.class);

        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        // проверяем статистику по счётчикам
        log.info("=====================COUNTERS=====================");
        Counter cntMal = job.getCounters().findCounter(CounterType.MALFORMED);
        Counter cntNF = job.getCounters().findCounter(CounterType.METRICNF);
        log.info(cntMal.getName() + ": " + cntMal.getValue());
        log.info(cntNF.getName() + ": " + cntNF.getValue());
    }
}
