package shlabs.lab1;

import java.io.File;
import java.io.FileWriter;
import java.util.Random;

/**
 * Генератор входных данных для работы
 * Ожидаемые аргументы:
 * 0. Имя директории для сохранения файла.
 * 1. Кол-во генерируемых строк данных
 * 2. Кол-во metricID. Предполагается, что номера метрик начинаются с 1 и идут непрерывно - например, 1-2-3.
 * 3. Масштаб в мс. Используется для определения макс. значения времени с целью обеспечения более "красивых" разбиений. Возможны любые значения.
 */
public class L1Gen {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) // Проверка аргументов
            throw new RuntimeException("You should specify output directory, the number of entries, the number of IDs, scale (in ms) ");
        final int groupsCnt = 10; // Ожидаемое количество "разбиений" в результате для каждого metricID
        final int maxMetrics = 1000; // Макс. значение метрики
        // Подготовить параметры для генератора
        int entries= Integer.parseInt(args[1]);
        int ids= Integer.parseInt(args[2]);
        int scale = Integer.parseInt(args[3]);
        long maxTime = (long)groupsCnt * (long)scale;
        Random rand = new Random(); //ГПСЧ
        // Если директория не существует, создать её
        new File(args[0]).mkdirs(); 
        // Генерация данных
        try (FileWriter writer = new FileWriter(args[0] + "/file.txt")) {
            for (int i = 0; i < entries; ++i)
                writer.write(rand.nextInt(ids) + 1 + ", " + Math.abs(rand.nextLong()) % maxTime + ", " + rand.nextInt(maxMetrics) + System.lineSeparator());
        }
        // Файл закрывается автоматически, т. к. это try-with-resources блок
    }
}
