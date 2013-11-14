/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.fs;

import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 *
 * @author alexmu
 */
public class VFSUtil {

    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");

    public static File getFile(String pFilePathStr) {
        File file = null;
        if (pFilePathStr.startsWith("hdfs")) {
            try {
                String localTmpFilePathStr = RuntimeEnv.getParam(RuntimeEnv.TMP_DATA_DIR) + File.separator + sdf.format(new Date()) + UUID.randomUUID();
                HDFSUtil.get(pFilePathStr, localTmpFilePathStr);
                file = new File(localTmpFilePathStr);
            } catch (Exception ex) {
                ex.printStackTrace();
                file = null;
            }
        } else if (pFilePathStr.startsWith("file")) {
            file = new File(pFilePathStr.replaceFirst("file://", ""));

        } else {
            file = new File("D:\\workspace\\cls-etl\\xml-sample-file.xml");
        }
        return file;
    }

    public static void putFile(String pSrcFilePathStr, String pDestFilePathStr) {
        if (pDestFilePathStr.startsWith("hdfs")) {
            try {
                HDFSUtil.put(pSrcFilePathStr, pDestFilePathStr);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
