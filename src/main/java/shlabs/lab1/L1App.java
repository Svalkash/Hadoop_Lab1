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

import java.io.File;
import java.io.FileReader;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.commons.io.FileUtils.deleteDirectory;

/**
 * Класс основного приложения
 */
@Log4j
public class L1App {

    /**
     * Функция, считывающая соответствие metricID и имён из CSV-файла.
     * Возвращает строку формата "номер1,имя1,номер2,имя2..." в случае успеха, или пустую строку при любых ошибках.
     * Сами ошибки выводятся в лог.
     */
    public static String readMetricIDs(String filename) {
        //"./src/main/resources/yourfile.csv"
        CSVReader reader;
        // Открытие файла
        try {
            reader = new CSVReader(new FileReader(filename));
        } catch (Exception ex) {
            log.fatal("CSV loading error: " + ex.getMessage());
            return "";
        }
        try {
            List<String[]> stringList = reader.readAll(); // Считываем все строки
            stringList.forEach((item)->{ if (item.length != 2) throw new RuntimeException("Wrong format"); }); //Проверка количества полей в каждой строке - должно быть 2
            if (stringList.size() == 0) // Проверка на пустой файл
                throw new RuntimeException("Empty CSV");
            Set<Integer> numSet = new LinkedHashSet<>();
            Set<String> nameSet = new LinkedHashSet<>();
            StringBuilder ret = new StringBuilder(); // Финальная строка
            for (String[] strings : stringList) {
                if (!numSet.add(Integer.parseInt(strings[0]))) // Проверка на одинаковые номера метрик
                    throw new RuntimeException("Duplicate metricIDs");
                if (!nameSet.add(strings[1]))
                    throw new RuntimeException("Duplicate metric names"); // Проверка на одинаковые имена метрик (иначе это не имеет смысла)
                ret.append(",").append(strings[0]).append(",").append(strings[1]); // Добавление к строке
            }
            return ret.substring(1); // Убираем ',' из начала
        }
        catch(NumberFormatException ex) { // Появляется, когда metricID - не число
            log.error("metricID is not a number: " + ex.getMessage());
            return "";
        }
        catch(Exception ex) { // Другие ошибки
            log.error("Exception while trying to read metricIDs config: " + ex.getMessage());
            return "";
        }
    }

    /**
     * Запуск Job с заданной конфигурацией и входной/выходной директорией. Очистка директорий не выполняется - это надо делать снаружи.
     * Возвращает время выполнения Job в мс.
     */
    public static long runJob(Configuration conf, Path inputDir, Path outputDir) throws Exception {
        Job job = Job.getInstance(conf, "Lab1Job");
        // Установка классов приложения, маппера и редьюсера
        job.setJarByClass(L1App.class);
        job.setMapperClass(L1Mapper.class);
        job.setReducerClass(L1Reducer.class);
        // Формат выходных данных. Чтобы соответствовать заданию, можно было пихать ВСЮ строку в ключ, но это бы имело мало смысла.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // Формат выходного файла - SequenceFile
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // Установка входной и выходной директорий
        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);
        log.info("=====================JOB STARTED=====================");
        // Запуск приложения
        long start = System.currentTimeMillis(); // Время начала
        job.waitForCompletion(true);
        long finish = System.currentTimeMillis(); // Время окончания
        long timeElapsed = finish - start; // Время работы (мс)
        log.info("=====================JOB ENDED - " + timeElapsed + " seconds =====================");
        log.info("=====================COUNTERS=====================");
        // Вывод значений счётчиков
        Counter cntMal = job.getCounters().findCounter(CounterType.MALFORMED); 
        Counter cntNF = job.getCounters().findCounter(CounterType.METRICNF);
        log.info(cntMal.getName() + ": " + cntMal.getValue());
        log.info(cntNF.getName() + ": " + cntNF.getValue());
        return timeElapsed;
    }

    /**
     * Одноразовый запуск runJob() с заданными параметрами.
     * Перед запуском удаляется директория output во избежание конфликтов.
     * Ожидаемые аргументы:
     * 0. Директория с входными данными
     * 1. Адрес директории для выходных данных (если существует, перезаписывается).
     * 2. Файл с расшифровкой metricID в имена.
     * 3. Масштаб. Поддерживаются значения: 1s, 10s, 1m, 10m, 1h, 1d.
     * 4. Функция для агрегации в редьююсере - avg, min, max.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 5) { // Проверка аргументов
            throw new RuntimeException("You should specify input and output folders, metcisIDs filename, scale and function!");
        }
        // Чтение файла с именами metricID
        String metricIDs = readMetricIDs(args[2]);
        if (Objects.equals(metricIDs, "")) { //Проверка на ошибки при чтении
            log.fatal("Couldn't read metricIDs, stopping.");
            return;
        }
        // Настройка объекта Configuration
        Configuration conf = new Configuration();
        conf.setStrings("metricIDs", metricIDs);
        conf.setStrings("scale", args[3]);
        conf.setStrings("function", args[4]);
        // Очистка выходной директории, если она существует
        deleteDirectory(new File(args[1]));
        // Запуск
        runJob(conf, new Path(args[0]), new Path(args[1]));
    }
}
