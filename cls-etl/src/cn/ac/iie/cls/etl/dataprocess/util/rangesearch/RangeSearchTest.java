/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.rangesearch;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.List;

/**
 *
 * @author alexmu
 */
public class RangeSearchTest {

    public static void main(String[] args) {
//        RangeSearch rs = new RangeSearch();
//        long st = System.nanoTime();
//        for (int i = 0; i < 24000; i += 2) {
//            rs.append(i, i + 1, i + "", i + "");
//        }
//
//        rs.contructArray();
//        long et = System.nanoTime();
//        System.out.println((et - st) / 1000000);
//
//        String val = rs.getValue(2, "2");
//        System.out.println(val);
//
//       
//        try {
//            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("D:\\objectFile.obj"));
//            out.writeObject(rs);
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }

        try {
            ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream("D:\\objectFile.obj")));

            long st = System.nanoTime();
            RangeSearch rs1 = (RangeSearch) in.readObject();
            long et = System.nanoTime();
            System.out.println((et - st) / 1000000);
            String val = rs1.getValue(10000, "10000");
            System.out.println(val);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
