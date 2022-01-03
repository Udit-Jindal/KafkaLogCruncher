package com.sta.kafka.logscruncher;

import com.sta.utils.dfs.FsUtils;
import de.vandermeer.asciitable.AsciiTable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

public class LogsCruncher
{

    public static void main(String[] args) throws IOException
    {
        new LogsCruncher().saveStats(args[0]);
    }

    private void saveStats(String inputFilePath) throws FileNotFoundException, IOException
    {
        Map<String, ProducerStats> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        Set<String> fileNames = FsUtils.getFileNames(inputFilePath);

        for (String fileName : fileNames)
        {
            String name = null;

            Long startTime = null;
            Long endTime = null;

            int records = 0;

            float rate = 0;

            int warnings = 0;

            boolean error = false;

            try (Scanner sc = new Scanner(new FileInputStream(inputFilePath + fileName)))
            {
                while (sc.hasNextLine())
                {
                    String line = sc.nextLine();

                    if (line.contains("WARN"))
                    {
                        warnings++;
                        continue;
                    }

                    if (line.contains("Exception"))
                    {
                        error = true;
                        continue;
                    }

                    if (line.contains("Producer Number"))
                    {
                        name = line.replaceAll("Producer Number:", "").trim();
                        continue;
                    }

                    if (line.contains("Producer Started At:"))
                    {
                        String st = line.replaceAll("Producer Started At:", "").trim();

                        startTime = Long.parseLong(st);

                        continue;
                    }

                    if (line.contains("Producer Finished At:"))
                    {
                        String st = line.replaceAll("Producer Finished At:", "").trim();

                        endTime = Long.parseLong(st);

                        continue;
                    }

                    if (line.contains("99.9th"))
                    {
                        String arr[] = line.split(",");

                        records = Integer.parseInt(arr[0].replaceAll("records sent", "").trim());
                        rate = Float.parseFloat(arr[1].substring(0, arr[1].indexOf("records/sec")).trim());
                    }
                }
            }

            map.put(name, new ProducerStats(name, (int) ((endTime - startTime)), records, rate, warnings, error));
        }

        int producers = 0;
        int totalRecords = 0;
        int maxTimeTaken = 0;

        int totalWarning = 0;
        int totalError = 0;

        AsciiTable at = new AsciiTable();

        at.addRule();
        at.addRow("Producer", "Records", "Time Taken", "Rate", "Warnings", "Errors");
        at.addRow("", "", "", "", "", "");

        for (Map.Entry<String, ProducerStats> entry : map.entrySet())
        {
            at.addRule();
            at.addRow(entry.getValue()._name, entry.getValue()._records, entry.getValue()._timeTaken, entry.getValue()._rate, entry.getValue().getWarnings(), entry.getValue().isError());

            producers++;
            totalRecords += entry.getValue()._records;

            if (maxTimeTaken < entry.getValue()._timeTaken)
            {
                maxTimeTaken = entry.getValue()._timeTaken;
            }

            totalWarning += entry.getValue().getWarnings();

            if (entry.getValue().isError())
            {
                totalError++;
            }
        }

        at.addRule();
        at.addRow("", "", "", "", "", "");
        at.addRule();
        at.addRow(producers, totalRecords, maxTimeTaken, (totalRecords * 1.0 / maxTimeTaken), totalWarning, totalError);
        at.addRow("", "", "", "", "", "");
        at.addRule();

        System.out.println(at.render());
    }

    private class ProducerStats
    {

        String _name;
        int _timeTaken;
        int _records;

        float _rate;

        int _warnings;
        boolean _error;

        public ProducerStats(String name, int timeTaken, int records, float rate, int warnings, Boolean error)
        {
            _name = name;
            _timeTaken = timeTaken;
            _records = records;
            _rate = rate;
            _warnings = warnings;
            _error = error;
        }

        public String getName()
        {
            return _name;
        }

        public int getTimeTaken()
        {
            return _timeTaken;
        }

        public int getRecords()
        {
            return _records;
        }

        public float getRate()
        {
            return _rate;
        }

        public int getWarnings()
        {
            return _warnings;
        }

        public boolean isError()
        {
            return _error;
        }

    }
}
