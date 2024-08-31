package com.emolokov.obc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;

public class FileGenerator_Baseline {

    public static void main(String[] args) throws IOException {
        File file = new File("obc_source.txt");
        FileWriter fileWriter = new FileWriter(file);
        Random random = new Random();

        long startTime = System.currentTimeMillis();
        List<String> cities = new ArrayList<>(1000);
        for(int i = 0; i < 1000; i++){
            String city = new String(Base64.getEncoder().encode(Long.valueOf(random.nextLong()).toString().getBytes())).toLowerCase().replaceAll("[^a-z0-9]+", "");
            city = city.substring(0, 10 + random.nextInt(city.length() - 10));
            cities.add(city);
        }

        for(long i = 0; i < 1_000_000_000L; i++){
            if(i % 1_000_000 == 0){
                System.out.println(STR."Generating line \{i / 1_000_000}M");
            }

            String city = cities.get(random.nextInt(cities.size()));

            String line = String.format("%s;%s%s.%s\n",
                    city,
                    random.nextBoolean() ? "-" : "",
                    random.nextInt(0, 40),
                    random.nextInt(0, 10));

            fileWriter.write(line);
        }

        fileWriter.flush();
        fileWriter.close();

        System.out.println(STR."Executed in \{System.currentTimeMillis() - startTime} ms");
    }
}
