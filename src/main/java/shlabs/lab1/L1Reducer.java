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
    String scaleStr; //Масштаб в изначальном формате (строка). Присутствует в каждой строке выходных данных по условию.
    String funStr; // Выполняемая функция для агрегации
    static final String[] funNames = { "avg", "max", "min" }; // Возможные значения funStr для проверки.

    /**
     * Функция, вызываемая в начале работы.
     * Считывает параметры из объекта конфигурации.
     */
    @Override
    protected void setup(Context context) {
        scaleStr = context.getConfiguration().getStrings("scale")[0]; // Масштаб
        funStr = context.getConfiguration().getStrings("function")[0]; // Функция
        if (!ArrayUtils.contains(funNames, funStr)) { // Проверка допустимости значения функции.
            log.fatal("Wrong function name provided");
            System.exit(1);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int result = 0;
        int cnt = 0;
        int tmp;
        boolean first = true;
        //Итерация по массиву значений
        for (IntWritable value : values) {
            tmp = value.get();
            // Применение функции
            switch (funStr) {
                case "max":
                    if (tmp > result || first)
                        result = tmp;
                    break;
                case "min":
                    if (tmp < result || first)
                        result = tmp;
                    break;
                case "avg":
                default:
                    result += tmp;
                    ++cnt;
                    break;
            }
            if (first)
                first = false;
        }
        // Финальные вычисления для avg
        switch(funStr) {
            case "avg":
                result /= cnt;
                break;
            default:
                break;
        }
        // Формирование финальной строки - добавляем масштаб в строковом виде.
        // Непонятно, зачем, но сказано в условии, что нужно.
        context.write(new Text(key.toString() + ", " + scaleStr), new IntWritable(result));
    }
}
