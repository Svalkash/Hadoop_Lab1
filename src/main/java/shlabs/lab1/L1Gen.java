package shlabs.lab1;

import java.io.FileWriter;
import java.util.Random;

public class L1Gen {

    public static void main(String[] args) throws Exception {

        if (args.length < 4)
            throw new RuntimeException("You should specify output file, the number of entries, the number of IDs, scale (in ms) ");
        final int groupsCnt = 10;
        final int maxMetrics = 1000;
        int entries= Integer.parseInt(args[1]);
        int ids= Integer.parseInt(args[2]);
        int scale = Integer.parseInt(args[3]);
        long maxTime = (long)groupsCnt * (long)scale;

        Random rand = new Random();
        try (FileWriter writer = new FileWriter(args[0])) {
            for (int i = 0; i < entries; ++i)
                writer.write(rand.nextInt(ids) + 1 + ", " + Math.abs(rand.nextLong()) % maxTime + ", " + rand.nextInt(maxMetrics) + System.lineSeparator());
        }
    }
}
