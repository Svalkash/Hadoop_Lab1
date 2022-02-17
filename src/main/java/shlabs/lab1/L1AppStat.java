package shlabs.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.awt.*;
import java.io.File;
import java.util.*;

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static shlabs.lab1.L1App.readMetricIDs;
import static shlabs.lab1.L1App.runJob;

@Log4j
public class L1AppStat {
    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            throw new RuntimeException("You should specify input and output folders, metcisIDs filename, scale and function! Also step and max number of entries.");
        }
        String metricIDs = readMetricIDs(args[2]);
        if (Objects.equals(metricIDs, "")) {
            log.fatal("Couldn't read metricIDs, stopping.");
            return;
        }
        Configuration conf = new Configuration(); //default config
        conf.setStrings("metricIDs", metricIDs);
        conf.setStrings("scale", args[3]);
        conf.setStrings("function", args[4]);

        XYSeries series = new XYSeries("Time");
        for (int cnt = 0; cnt < Integer.parseInt(args[6]); cnt += Integer.parseInt(args[5])) {
            log.info("======= RUNNING APP FOR " + cnt + " ENTRIES ========");
            L1Gen.main(new String[] { args[0], Integer.toString(cnt), Integer.toString(3), Long.toString(L1Mapper.scaleToLong(args[3]))}); //generate data
            deleteDirectory(new File(args[1]));//delete output folder before starting
            series.add(cnt, runJob(conf, new Path(args[0]), new Path(args[1])));
        }

        XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(series);
        EventQueue.invokeLater(() -> {
            LineChartEx ex = new LineChartEx(dataset);
            ex.setVisible(true);
        });
    }


}
