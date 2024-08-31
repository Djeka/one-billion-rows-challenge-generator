package com.emolokov.obrc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FileGenerator_Optimized_Multithreaded {

    private static final int LINES_COUNT = 1000_000_000;
    private static final int CITIES_COUNT = 1000;
    private static final char[][] cities = new char[CITIES_COUNT][];
    static {
        Random random = new Random();
        for(int i = 0; i < CITIES_COUNT; i++){
            String city = new String(Base64.getEncoder().encode(Long.valueOf(random.nextLong()).toString().getBytes())).toLowerCase().replaceAll("[^a-z0-9]+", "");
            city = city.substring(0, 10 + random.nextInt(city.length() - 10));

            byte[] cityNameBytes = city.getBytes();
            char[] cityNameChars = new char[city.length()];
            for(int c = 0; c < cityNameChars.length; c++) cityNameChars[c] = (char) cityNameBytes[c];
            cities[i] = cityNameChars;
        }
    }

    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();

        Writer writer = new Writer();
        int genCount = Math.max(1, Runtime.getRuntime().availableProcessors() - 1); // use all provided cpu cores
        Generator[] generators = new Generator[genCount];
        for (int i = 0; i < genCount; i++) {
            generators[i] = new Generator(i);
        }

        ExecutorService executor = Executors.newFixedThreadPool(genCount + 1);
        executor.execute(writer);
        for(Generator generator: generators){
            executor.execute(generator);
        }
        executor.shutdown();

        boolean success;
        try {
            success = executor.awaitTermination(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to wait for execution completion");
        }

        if(!success){
            throw new RuntimeException(STR."Execution terminated by timeout of 5 min");
        }

        for(Generator generator: generators){
            System.out.println(STR."[Gen \{generator.genId}] wasted \{generator.wastedTime} ms");
        }

        System.out.println(STR."Executed in \{System.currentTimeMillis() - startTime} ms");
    }

    private enum BufferState {
        WRITE_IN_PROGRESS,
        WRITE_COMPLETE,
        GEN_IN_PROGRESS,
        GEN_COMPLETE
    }

    private static final int BUFFERS_COUNT = 512;
    private static final int BUFFER_SIZE = 2 * 1024 * 1024; // bytes
    private static final char[][] buffers = new char[BUFFERS_COUNT][BUFFER_SIZE];
    private static final int[] bufferLen = new int[BUFFERS_COUNT];
    private static final int[] bufferLinesCount = new int[BUFFERS_COUNT];
    private static final BufferState[] bufferState = new BufferState[BUFFERS_COUNT];
    private static final AtomicInteger bufferSelector = new AtomicInteger(); // last handled chunk number
    private static int writeBufferIdx = 0;
    private static boolean writeComplete = false;

    private static class Writer implements Runnable {

        @Override
        public void run() {
            try {
                File file = new File("generated.txt");
                FileWriter fileWriter = new FileWriter(file);

                int linesCount = LINES_COUNT;
                int generatedLines = 0;
                while (true){
                    char[] buffer = buffers[writeBufferIdx];

                    int bufferLength = bufferLen[writeBufferIdx];
                    if(linesCount - bufferLinesCount[writeBufferIdx] < 0){
                        bufferLength = 0;
                        int count = linesCount;
                        for(int i = 0; i < bufferLen[writeBufferIdx] && count > 0; i++){
                            if(buffer[i] == '\n'){
                                count--;
                            }
                            bufferLength++;
                        }
                    }

                    fileWriter.write(buffer, 0, bufferLength);
                    linesCount -= bufferLinesCount[writeBufferIdx];

                    if((LINES_COUNT - linesCount) / 1_000_000 > generatedLines){
                        generatedLines = (LINES_COUNT - linesCount) / 1_000_000;
                        System.out.println(STR."Flushed \{generatedLines}M lines");
                    }

                    if(linesCount <= 0){
                        writeComplete = true;
                        break;
                    }

                    int priorBufferIdx = writeBufferIdx;
                    writeBufferIdx = (writeBufferIdx + 1) % BUFFERS_COUNT;

                    bufferState[writeBufferIdx] = BufferState.WRITE_IN_PROGRESS;
                    bufferState[priorBufferIdx] = BufferState.WRITE_COMPLETE;
                }

                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class Generator implements Runnable {

        private int genId;
        private int bufferIdx;
        private char[] buffer;
        private SplittableRandom random = new SplittableRandom();;
        private long wastedTime;

        public Generator(int genId) {
            this.genId = genId;
        }

        @Override
        public void run() {
            while (true){
                if(writeComplete){
                    return;
                }

                this.bufferIdx = bufferSelector.getAndIncrement() % BUFFERS_COUNT;
                this.buffer = buffers[bufferIdx];

                if(bufferState[bufferIdx] != BufferState.WRITE_COMPLETE){
                    long startWasteTime = System.currentTimeMillis();
                    try {
                        while (bufferState[bufferIdx] != BufferState.WRITE_COMPLETE){
                            if(writeComplete){
                                return;
                            }
                            Thread.sleep(1);
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    wastedTime += System.currentTimeMillis() - startWasteTime;
                }

                bufferState[bufferIdx] = BufferState.GEN_IN_PROGRESS;

                int pos = 0;
                int linesCount = 0;
                while (true){
                    long randLong = random.nextLong();
                    char[] city = cities[(int) Math.abs(randLong % CITIES_COUNT)];

                    if(buffer.length - pos - 1 < city.length + 7){ // city + ";-40.9\n" of length 7
                        break;
                    }

                    linesCount++;

                    System.arraycopy(city, 0, buffer, pos, city.length);
                    pos += city.length;
                    buffer[pos++] = ';';

                    long sign = (randLong & 0b1);
                    randLong = randLong >> 8;
                    long firstDigit = ( randLong & 0b11111111) % 10;
                    randLong = randLong >> 8;
                    long secondDigit =( randLong & 0b11111111) % 10;
                    randLong = randLong >> 8;
                    long floatDigit =( randLong & 0b11111111) % 10;

                    if(sign == 1L){
                        buffer[pos++] = '-';
                    }

                    if(firstDigit != 0){
                        buffer[pos++] = (char) (firstDigit + '0');
                    }

                    buffer[pos++] = (char) (secondDigit + '0');
                    buffer[pos++] = '.';
                    buffer[pos++] = (char) (floatDigit + '0');
                    buffer[pos++] = '\n';
                }

                bufferLen[bufferIdx] = pos + 1;
                bufferLinesCount[bufferIdx] = linesCount;
                bufferState[bufferIdx] = BufferState.GEN_COMPLETE;
            }
        }
    }
}
