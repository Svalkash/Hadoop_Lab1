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

/**
     * Многоразовый запуск runJob() с разным кол-вом строк входных данных.
     * В каждом запуске используются одни и те же директории.
     * В конце строится график времени выполнения от объёма данных
     * Ожидаемые аргументы:
     * 0. Директория с входными данными
     * 1. Адрес директории для выходных данных (если существует, перезаписывается).
     * 2. Файл с расшифровкой metricID в имена.
     * 3. Масштаб. Поддерживаются значения: 1s, 10s, 1m, 10m, 1h, 1d.
     * 4. Функция для агрегации в редьююсере - avg, min, max.
     * 5. Шаг (увеличение кол-ва данных между двумя запусками)
     * 6. Максимальное кол-во данных.
     */
@Log4j
public class L1AppStat {
    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            throw new RuntimeException("You should specify input and output folders, metcisIDs filename, scale and function! Also step and max number of entries.");
        }
        // Чтение файла с именами metricID
        // Файл читается один раз в начале работы.
        String metricIDs = readMetricIDs(args[2]); //Проверка на ошибки при чтении
        if (Objects.equals(metricIDs, "")) {
            log.fatal("Couldn't read metricIDs, stopping.");
            return;
        }
        // Создание объекта Configuration и установка общих параметров для всех запусков
        Configuration conf = new Configuration();
        conf.setStrings("metricIDs", metricIDs);
        conf.setStrings("scale", args[3]);
        conf.setStrings("function", args[4]);
        // Создание массива данных для будущего графика
        XYSeries series = new XYSeries("Time");
        // Запуск
        for (int cnt = 0; cnt < Integer.parseInt(args[6]); cnt += Integer.parseInt(args[5])) {
            log.info("======= RUNNING APP FOR " + cnt + " ENTRIES ========");
            // Генерация входных данных
            L1Gen.main(new String[] { args[0], Integer.toString(cnt), Integer.toString(3), Long.toString(L1Mapper.scaleToLong(args[3]))}); //generate data
            // Очистка выходной директории
            deleteDirectory(new File(args[1]));
            // Добавление точки для графика.
            series.add(cnt, runJob(conf, new Path(args[0]), new Path(args[1])));
        }
        // Вывод графика
        XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(series);
        EventQueue.invokeLater(() -> {
            LineChartEx ex = new LineChartEx(dataset);
            ex.setVisible(true);
        });
    }


}
