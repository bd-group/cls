/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.fs.csvfileparser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author lbh
 */
public class CSVFileParser {

    protected File csvFile = null;
    protected boolean hasHeader = false;
    protected boolean trimLines = false;
    protected String fileEncoding = "";
    protected String enclosure = "";
    protected String delimiter = "";
    protected BufferedReader br = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(CSVFileParser.class.getName());
    }

    public CSVFileParser(File pCsvFile, String pDelimiter, String pEnclosure, String pFileEncoding, boolean pHasHeader, boolean pTrimLines) {
        csvFile = pCsvFile;
        hasHeader = pHasHeader;
        trimLines = pTrimLines;
        fileEncoding = pFileEncoding;
        enclosure = pEnclosure;
        delimiter = pDelimiter;
    }

    public String[] getNext() throws Exception {
        String[] aryLine = null;
        try {
            if (br == null) {
                br = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), fileEncoding));
            }
        } catch (Exception ex) {
            logger.info("file is null");
            throw new Exception("file is null" + ex.getMessage(), ex);
        }

        try {
            String line = null;
            if (hasHeader) {
                line = br.readLine();
                hasHeader = false;
            }
            line = br.readLine();
            if (line == null) {   //文件结束
                logger.info("end-of-file");
                aryLine = null;
            } else {
//                if (line.isEmpty()) {//空记录行的处理
//                    line = br.readLine();
//                    if (line == null) {  //文件结束
//                        logger.info("end-of-file");
//                        return null;
//                    }
//                }
                if ("\\t".equalsIgnoreCase(delimiter)) {
                    delimiter = "\t";
                }
                List<String> tokens = new ArrayList<String>();
                tokens = enclosureSplit(line, delimiter.charAt(0), enclosure.charAt(0), enclosure.charAt(0));
                aryLine = tokens.toArray(new String[tokens.size()]);
                for (int i = 0; i < aryLine.length; i++) {
                    if (aryLine[i].startsWith(enclosure)) {
                        aryLine[i] = aryLine[i].substring(1, aryLine[i].length() - 1);
                    }
                    if (aryLine[i].isEmpty() || aryLine[i].equalsIgnoreCase("null") || aryLine[i].equalsIgnoreCase("\\n")) {
                        aryLine[i] = null;
                    }
                    
                }
               
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.info("Reading file is failed");
            throw new Exception("Reading file is failed  for" + ex.getMessage(), ex);
        }
        return aryLine;
    }

    private List<String> enclosureSplit(String data, char separator,
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

    private String decodeEnclosure(String data, char beginEnclosure, char endEnclosure) {
        if (data.isEmpty()) {
            return data;
        }
        if (data.charAt(0) != beginEnclosure) {
            return data;
        }
        StringBuilder sbuf = new StringBuilder();
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

    public void close() throws Exception {
        try {
            br.close();
        } catch (Exception ex) {
            logger.warn("InputFile is null or Creating file stream is failed for " + ex.getMessage());
            throw new Exception("InputFile is null or Creating file stream is failed for " + ex.getMessage(), ex);
        }
    }

    public static void main(String[] args) {
        File file = new File("csv-sample-file.csv");
        CSVFileParser readFileUtil = new CSVFileParser(file, ",", "\"", "utf-8", false, false);
        String[] aryLine = null;
        try {
            while ((aryLine = readFileUtil.getNext()) != null) {
                for (int i = 0; i < aryLine.length; i++) {
                    System.out.print(aryLine[i] + "     ");
                }
                System.out.println();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
