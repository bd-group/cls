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
//            System.out.println("1111\t" + line);
            if (line == null) {   //文件结束
                logger.info("end-of-file");
                aryLine = null;
            } else {
                if (trimLines) {
                    line = line.trim();
                }
//                System.out.println("0000000000\t"+line);
                if (line.isEmpty()) {//空记录行的处理
                    line = br.readLine();
                    if (line == null) {  //文件结束
                        logger.info("end-of-file");
                        return null;
                    }
                }
                if (delimiter.length() == 1) {
                    char pattern = delimiter.charAt(0);
                    if (pattern == '|' || pattern == '\\' || pattern == '*' || pattern == '\'' //考虑单个字符为正则表达式中转义字符的情况
                            || pattern == '\"' || pattern == '+' || pattern == '^'
                            || pattern == '$' || pattern == '?' || pattern == '.') //ASK--- |,\,*,',",+,^,$,?,.
                    {
                        delimiter = "\\" + delimiter;
                    }
                }
                if ("".equals(enclosure)) {   //字段不涵包围符
                    line = line.replaceAll("\\\\" + delimiter, (char) (delimiter.charAt(0) + 3) + "_-");
                    aryLine = line.split(delimiter, -1);
                    if (delimiter.equalsIgnoreCase(",")) {
                        aryLine = forDealWithSplit(aryLine);
                    }

                    for (int i = 0; i < aryLine.length; i++) {
                        if ("".equals(aryLine[i].trim()) || aryLine[i].trim().equalsIgnoreCase("null") || aryLine[i].trim().equalsIgnoreCase("\\n")) {
                            aryLine[i] = null;
                        } else if (aryLine[i].startsWith("\"")) {
                            aryLine[i] = aryLine[i].replaceAll((char) (delimiter.charAt(0) + 3) + "_-", delimiter);
                            aryLine[i] = aryLine[i].replaceAll("\\\\", "");
                            aryLine[i] = aryLine[i].substring(1, aryLine[i].length() - 1);
                        } else {
                            aryLine[i] = aryLine[i].replaceAll((char) (delimiter.charAt(0) + 3) + "_-", delimiter);
                            aryLine[i] = aryLine[i].replaceAll("\\\\", "");
                        }
                    }
                } else {
                    aryLine = line.split(enclosure + delimiter + enclosure);
                    aryLine[0] = aryLine[0].substring(1, aryLine[0].length());
                    aryLine[aryLine.length - 1] = aryLine[aryLine.length - 1].substring(0, aryLine[aryLine.length - 1].length() - 1);
                    for (int i = 0; i < aryLine.length; i++) {
                        if ("".equals(aryLine[i].trim())) {
                            aryLine[i] = null;
                        } else {
                            aryLine[i] = aryLine[i].replaceAll("\\\\", "");
                        }
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

    private String[] forDealWithSplit(String[] pAryLine) {
        StringBuffer sb = new StringBuffer();
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < pAryLine.length; i++) {
            if (pAryLine[i].startsWith("\"") && !pAryLine[i].endsWith("\"")) {
                for (int j = i + 1; j < pAryLine.length; j++) {
                    pAryLine[i] += "," + pAryLine[j];
                    sb.append(j + ",");
                    if (pAryLine[j].endsWith("\"")) {
                        break;
                    }
                }
            }
            list.add(pAryLine[i]);
        }
        if (sb.toString().length() != 0) {
            String[] toRemoveAry = sb.toString().split(",");
            for (int i = toRemoveAry.length - 1; i >= 0; i--) {
                list.remove(Integer.parseInt(toRemoveAry[i]));
            }
            pAryLine = list.toArray(new String[1]);
//            for (int i = 0; i < pAryLine.length; i++) {
//                System.out.println("00000000\t" + pAryLine[i]);
//            }
        }
        return pAryLine;
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
        File file = new File("E:\\test\\test1.txt");
        CSVFileParser readFileUtil = new CSVFileParser(file, ",", null, "utf-8", false, false);
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
