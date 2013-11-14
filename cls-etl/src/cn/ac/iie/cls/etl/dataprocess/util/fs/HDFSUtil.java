/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.fs;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class HDFSUtil {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(HDFSUtil.class.getName());
    }
    static Map<String, FileSystem> hdfsSet = new HashMap<String, FileSystem>();

    public static void put(String pSrcFilePathStr, String pHDFSFilePathStr) throws Exception {
        try {
            FileSystem fs = getHDFS(pHDFSFilePathStr);
            Path srcPath = new Path(pSrcFilePathStr);
            Path dstPath = new Path(pHDFSFilePathStr);
            if (fs.isFile(dstPath)) {
            } else {
                fs.mkdirs(dstPath);
            }
            fs.copyFromLocalFile(srcPath, dstPath);
        } catch (Exception ex) {
            logger.warn("put " + pSrcFilePathStr + " to hdfs(" + pHDFSFilePathStr + ") unsuccessfully for " + ex.getMessage(), ex);
            throw ex;
        } 
    }

    public static FileSystem getHDFS(String pHDFSFilePathStr) {
        if (pHDFSFilePathStr == null || pHDFSFilePathStr.isEmpty()) {
            return null;
        } else {
            URI hdfsURI = URI.create(pHDFSFilePathStr);
            String hdfsID = hdfsURI.getHost() + ":" + hdfsURI.getPort();
            synchronized (hdfsSet) {
                FileSystem fs = hdfsSet.get(hdfsID);
                if (fs == null) {
                    Configuration conf = new Configuration();
                    try {
                        fs = FileSystem.get(hdfsURI, conf);
                        hdfsSet.put(hdfsID, fs);
                    } catch (Exception ex) {
                        logger.warn("get hdfs of " + pHDFSFilePathStr + "unsuccessfully for " + ex.getMessage(), ex);
                        fs = null;
                    }
                }
                return fs;
            }
        }
    }

    public static void get(String pHDFSFilePathStr, String pLocalFilePathStr) throws Exception {
        try {
            FileSystem fs = getHDFS(pHDFSFilePathStr);
            fs.copyToLocalFile(new Path(pHDFSFilePathStr), new Path(pLocalFilePathStr));
        } catch (Exception ex) {
            logger.warn("get " + pHDFSFilePathStr + " to localfs(" + pLocalFilePathStr + ") unsuccessfully for " + ex.getMessage(), ex);
            throw ex;
        }
    }
}
