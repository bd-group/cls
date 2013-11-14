/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.fs.csvfileparser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author root
 */
public class CSVFileParserTest {

    public static void main(String[] args) {
        File file = new File("csv-sample-file.csv");
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "utf-8"));
            List<String> tokens = new LinkedList<String>();
            String line = br.readLine();
            char separator = "\t".charAt(0);
            char beginEnclosure = '"';
            System.out.println(separator);
            char endEnclosure = '"';
            tokens = enclosureSplit(line, separator, beginEnclosure, endEnclosure);
//            tokens=enclosureSplit(line,separator,' ',' ');
            System.out.println(tokens.size());
            String[] arrLine = new String[tokens.size()];
            System.out.println(arrLine.length);
            arrLine = tokens.toArray(new String[tokens.size()]);
//            String[] arrLine=null;
//            arrLine=line.split("\t");
            for (int i = 0; i < arrLine.length; i++) {
                if (arrLine[i].startsWith(beginEnclosure + "")) {
                    arrLine[i] = arrLine[i].substring(1, arrLine[i].length() - 1);
                }
                if (arrLine[i].isEmpty() || arrLine[i].trim().equalsIgnoreCase("null") || arrLine[i].trim().equalsIgnoreCase("\\n")) {
                    arrLine[i] = null;
                }

            }
            
            for (int i = 0; i < arrLine.length; i++) {
                System.out.print("arrLine[" + i + "]=" + arrLine[i] + "\t");
            }
            System.out.println();
            System.out.println(arrLine.length);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static final List<String> enclosureSplit(String data, char separator,
            char beginEnclosure, char endEnclosure) {
        int match = 0;
        int startOffset = 0;
        boolean beginEQend = (beginEnclosure == endEnclosure);
        List<String> tokens = new LinkedList<String>();

        for (int i = 0; i < data.length(); i++) {
            if (data.charAt(i) == beginEnclosure) {
                if (beginEQend && match != 0) {
                    match--;
                } else {
                    match++;
                }
            } else if (data.charAt(i) == endEnclosure) {
                match--;
            } else if (data.charAt(i) == separator) {
                if (match == 0) {
                    tokens.add(decodeEnclosure(data.substring(startOffset, i), beginEnclosure, endEnclosure));
                    startOffset = i + 1;
                }
            }
        }
        if (startOffset != data.length()) {
            tokens.add(decodeEnclosure(data.substring(startOffset), beginEnclosure, endEnclosure));
        }
        return tokens;
    }

    protected static final String decodeEnclosure(String data, char beginEnclosure, char endEnclosure) {
        if (data.isEmpty()) {
            return data;
        }
        if (data.charAt(0) != beginEnclosure) {
            return data;
        }
        StringBuffer sbuf = new StringBuffer();
        sbuf.append(beginEnclosure);
        for (int i = 1; i < data.length() - 1; i++) {
            char c = data.charAt(i);
            if (c == beginEnclosure) {
                i++;
            } else if (c == endEnclosure) {
                i++;
            }
            sbuf.append(c);
        }
        sbuf.append(endEnclosure);
        return sbuf.toString();
    }
}
