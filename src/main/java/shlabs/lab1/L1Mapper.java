package shlabs.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Маппер: Проверяет корректность входных данных, определяет группу, формирует строку-ключ.
 */
@Log4j
public class L1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Map<Integer, String> metricIDs; // Соответствие номеров метрик именам
    long scale; // Масштаб в числовом формате (в мс).

    /**
     * Считывание расшифровок имён metricID из строки.
     * Предполагается, что строка правильно отформатирована и содержит неповторяющиеся данные.
     * Ошибочные ситуации должны быть обработаны при чтении этой строки из CSV.
     * Возвращает объект Map с ассоциациями.
     */
    public static Map<Integer, String> metricStringToMap(String[] strArr) {
        Map<Integer, String> ret = new HashMap<>();
        for (int i = 0; i < strArr.length; i += 2)
            ret.put(Integer.parseInt(strArr[i]), strArr[i + 1]);
        return ret;
    }

    /**
     * Конвертация масштаба из строкового формата в числовой.
     * Возвращает масштаб в мс, или 0, если значение не поддерживается.
     */
    public static long scaleToLong(String scaleStr) {
        return scaleStr.equals("1s") ? 1000 :
            scaleStr.equals("10s") ? 10000 :
                    scaleStr.equals("1m") ? 60000 :
                            scaleStr.equals("10m") ? 60000 :
                                    scaleStr.equals("1h") ? 360000 :
                                            scaleStr.equals("1d") ? 8640000 :
                                                    0;
    }

    /**
     * Функция, вызываемая в начале работы.
     * Считывает параметры из объекта конфигурации.
     */
    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        metricIDs = metricStringToMap(conf.getStrings("metricIDs")); // Строка с расшифровкой metricID
        scale = scaleToLong(conf.getStrings("scale")[0]); // Масштаб
        if (scale == 0) { // Обработка ошибок
            log.fatal("Wrong scale value provided");
            System.exit(1);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        long[] result;

        try {
            // Предполагается, что входные данные - целые числа, разделённые запятыми с пробелами.
            // Любое несоответствие приводит к ошибке.
            result = Arrays.stream(value.toString().split(", ")).mapToLong(Long::parseLong).toArray();
        } catch (Exception ex) { // Если строку не получилось разбить на массив целых чисел.
            log.error(ex.getMessage());
            context.getCounter(CounterType.MALFORMED).increment(1);
            return;
        }
        if (result.length != 3) { // Если строка имеет некорректную длину
            context.getCounter(CounterType.MALFORMED).increment(1);
            return;
        }
        if (!metricIDs.containsKey((int)result[0])) { // Несуществующие метрики
            context.getCounter(CounterType.METRICNF).increment(1);
            return;
        }
        // Сохраняем извлечённые данные
        // Масштаб в строковом формате будет добавлен в редьюсере - минимизация пересылаемых данных.
        context.write(new Text(metricIDs.get((int)result[0]) + ", " + result[1] / scale * scale), new IntWritable((int)result[2]));
    }
}
