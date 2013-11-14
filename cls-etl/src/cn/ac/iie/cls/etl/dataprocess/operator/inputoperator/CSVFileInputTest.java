/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.inputoperator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author root
 */
public class CSVFileInputTest {
//    
//    1999,Chevy,"Venture ""Extended Edition""","",4900.00
//1999,Chevy,"Venture ""Extended Edition, Very Large""","",5000.00
//1996,Jeep,Grand Cherokee,"MUST SELL!
//air, moon roof, loaded",4799.00

    public static void main(String[] args) {
        File file = new File("/usr/iie/cls-v2/cls-etl/csv-sample-file.csv");
        BufferedReader br;
        List<String[]> list = new ArrayList<String[]>();
        String line;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "utf-8"));
            while ((line = br.readLine()) != null) {
                StringBuffer sb = new StringBuffer();
                List<String> tmplist = new ArrayList<String>();
                String[] strs = line.split(",");
                for (int i = 0; i < strs.length; i++) {
                    if (strs[i].startsWith("\"") && !strs[i].endsWith("\"")) {

                        for (int j = i + 1; j < strs.length; j++) {
                            //1997,Ford,E350,"ac, abs, moon",3000.00
                            // 0     1   2     3    4    5     6
                            strs[i] += "," + strs[j];
                            sb.append(j + ",");
                            if (strs[j].endsWith("\"")) {
                                break;
                            }
                        }
                    } else if (strs[i].startsWith("\"") && strs[i].endsWith("\"")) {
                        strs[i]=strs[i].substring(1, strs[i].length()-1);
                        strs[i]=strs[i].replaceAll("\"\"", "\"");
                    }
                    tmplist.add(strs[i]);
                }

                if (sb.toString().length() != 0) {
                    String[] toRemoveAry = sb.toString().split(",");
                    for (int i = toRemoveAry.length - 1; i >= 0; i--) {
                        tmplist.remove(Integer.parseInt(toRemoveAry[i]));
                    }
                    strs = tmplist.toArray(new String[1]);
                }
                for (int i = 0; i < strs.length; i++) {
                    if (strs[i].startsWith("\"")) {
                        strs[i] = strs[i].substring(1, strs[i].length() - 1);
                    }
                }
                list.add(strs);
            }
            for (int i = 0; i < list.get(1).length; i++) {
                System.out.println(list.get(0)[i]);
            }


        } catch (Exception ex) {
        }
    }
}
