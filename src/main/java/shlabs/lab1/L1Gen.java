package shlabs.lab1;

import java.io.File;
import java.io.FileWriter;
import java.util.Random;

public class L1Gen {

    public static void main(String[] args) throws Exception {

        if (args.length < 4)
            throw new RuntimeException("You should specify output directory, the number of entries, the number of IDs, scale (in ms) ");
        int groupsCnt = 10;
        final int maxMetrics = 1000;
        int entries= Integer.parseInt(args[1]);
        int ids= Integer.parseInt(args[2]);
        int scale = Integer.parseInt(args[3]);
        long maxTime = (long)groupsCnt * (long)scale;

        Random rand = new Random();
        new File(args[0]).mkdirs(); //create directory if not exists
        try (FileWriter writer = new FileWriter(args[0] + "/file.txt")) {
            for (int i = 0; i < entries; ++i)
                writer.write(rand.nextInt(ids) + 1 + ", " + Math.abs(rand.nextLong()) % maxTime + ", " + rand.nextInt(maxMetrics) + System.lineSeparator());
        }
    }
}
