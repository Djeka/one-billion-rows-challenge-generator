package com.emolokov.obc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class FileGenerator_Optimized {

    public static void main(String[] args) throws IOException {
        Random random = new Random();


        List<char[]> cities = new ArrayList<>(1000);
        for(int i = 0; i < 1000; i++){
            String city = new String(Base64.getEncoder().encode(Long.valueOf(random.nextLong()).toString().getBytes())).toLowerCase().replaceAll("[^a-z0-9]+", "");
            city = city.substring(0, 10 + random.nextInt(city.length() - 10));

            byte[] cityNameBytes = city.getBytes();
            char[] cityNameChars = new char[city.length()];
            for(int c = 0; c < cityNameChars.length; c++) cityNameChars[c] = (char) cityNameBytes[c];
            cities.add(cityNameChars);
        }


        generate(cities);

    }


    private static void generate(List<char[]> cities) throws IOException {
        int citiesSize = cities.size();

        long startTime = System.currentTimeMillis();

        File file = new File("obc_source.txt");
        FileWriter fileWriter = new FileWriter(file);
        SplittableRandom random = new SplittableRandom();

        char[] buffer = new char[2 * 1024 * 1024];
        int buffIdx = 0;
        for(long i = 0; i < 1000_000_000; i++){
            if(i % 1_000_000 == 0){
                System.out.println(STR."Generating line \{i / 1_000_000}M in \{System.currentTimeMillis() - startTime} ms");
            }

            long randLong = random.nextLong();

            char[] city = cities.get((int) Math.abs(randLong % citiesSize));

            if(buffer.length - buffIdx - 1 < city.length + 7){ // city + ";-40.9\n" of length 7
                fileWriter.write(buffer, 0, buffIdx);
                buffIdx = 0;
            }

            System.arraycopy(city, 0, buffer, buffIdx, city.length);
            buffIdx += city.length;
            buffer[buffIdx++] = ';';

            long sign = (randLong & 0b1);
            randLong = randLong >> 8;
            long firstDigit = ( randLong & 0b11111111) % 10;
            randLong = randLong >> 8;
            long secondDigit =( randLong & 0b11111111) % 10;
            randLong = randLong >> 8;
            long floatDigit =( randLong & 0b11111111) % 10;

            if(sign == 1L){
                buffer[buffIdx++] = '-';
            }

            if(firstDigit != 0){
                buffer[buffIdx++] = (char) (firstDigit + '0');
            }

            buffer[buffIdx++] = (char) (secondDigit + '0');
            buffer[buffIdx++] = '.';
            buffer[buffIdx++] = (char) (floatDigit + '0');
            buffer[buffIdx++] = '\n';

        }

        fileWriter.flush();
        fileWriter.close();

        System.out.println(STR."Executed in \{System.currentTimeMillis() - startTime} ms");
    }
}
