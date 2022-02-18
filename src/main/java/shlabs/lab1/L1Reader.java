package shlabs.lab1;

import com.opencsv.CSVReader;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.FileReader;
import java.io.FileWriter;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Расшифровщик SequenceFile.
 * Ожидаемые аргументы:
 * 0. Имя входного файла (не директории).
 * 1 (опционально). Имя выходного файла.
 * Если аргумент 1 отсутствует, данные выводятся в консоль.
 */
@Log4j
public class L1Reader {

    public static void main(String[] args) throws Exception {
        // Проверка аргументов
        if (args.length < 1)
            throw new RuntimeException("You should specify input (and optionally - output) files!");

        Configuration conf = new Configuration();
        Path inFile = new Path(args[0]);
        SequenceFile.Reader reader = null;
        FileWriter writer = null;
        try {
            Text key = new Text();
            IntWritable value = new IntWritable();
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(inFile), SequenceFile.Reader.bufferSize(4096));
            if (args.length > 1) // Если есть аргумент 1, открываем файл на запись
                writer = new FileWriter(args[1]);
            while (reader.next(key, value))
                if (args.length > 1) // Вывод в файл
                    writer.write("Key: " + key + " | Value: " + value + System.lineSeparator());
                else // Вывод в консоль
                    log.info("Key: " + key + " | Value: " + value);
        } catch (Exception ex) {
            log.fatal("Error: " + ex.getMessage());
        }
        if (reader != null)
            reader.close();
        if (writer != null)
            writer.close();
    }
}
