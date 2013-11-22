/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.outputoperator;

import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.util.fs.VFSUtil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;


/**
 *
 * @author alexmu
 */
public class TableOutputOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "errport1";
    private String dataSource;
    private String tableName;
    private boolean isClean;
    private int rowLimit;
    private List<Field2TableOutput> field2TableOutputSet = new ArrayList<Field2TableOutput>();
    private static Map<String, List<TableColumn>> tableSet = new HashMap<String, List<TableColumn>>();
    private String outputFormat = "";
    String tmpDataFileName = "";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(GlobalTableOuputOperator.class.getName());
    }

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    protected void init0() throws Exception {
    }

    public void validate() throws Exception {
        if (getPort(OUT_PORT).getConnector().size() < 1) {
            throw new Exception("out port with no connectors");
        }
    }

    @Override
	public void start()
   	{
   		// TODO Auto-generated method stub
   		synchronized (this)
   		{
   			notifyAll();
   		}
   	}
    
    protected void execute() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            tmpDataFileName = tableName + "_" + sdf.format(new Date());
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(tmpDataFileName))));
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();
                int dataSize = dataSet.size();
                for (int i = 0; i < dataSize; i++) {
                    Record record = dataSet.getRecord(i);
                    String outString = outputFormat;
                    for (Field2TableOutput field2TableOutput : field2TableOutputSet) {
                        String fieldVal = record.getField(field2TableOutput.streamFieldName).toString().replaceAll("\\\\", "\\\\\\\\\\\\\\\\");
                        fieldVal = fieldVal.replaceAll(",", "\\\\\\\\,");
                        outString = outString.replaceFirst(field2TableOutput.tableFieldName + "_REP", fieldVal);
                    }
                    outString = outString.replaceFirst("dms_update_time_REP", sdf.format(new Date()));
                    
                    bw.write(outString + "\n");
                }
                if (!dataSet.isValid()) {
                    bw.close();
                    break;
                }
            }
            status = SUCCEEDED;
        } catch (Exception ex) {
            status = FAILED;
            ex.printStackTrace();
        } finally {
        	synchronized (this) {
				try {
					wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
    		while (true) {
    			if (isClean) {
                    try {
    					MetaStoreProxy.truncateTable(dataSource, tableName);
    				} catch (Exception e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
                    VFSUtil.putFile(tmpDataFileName, RuntimeEnv.getParam(RuntimeEnv.HDFS_CONN_STR) + "/user/hive/warehouse/" + tableName);
                }
                VFSUtil.putFile(tmpDataFileName, RuntimeEnv.getParam(RuntimeEnv.HDFS_CONN_STR) + "/user/hive/warehouse/" + tableName);
                break;
    		}
        }
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        Iterator parameterItor = operatorElt.elementIterator("parameter");
        while (parameterItor.hasNext()) {
            Element parameterElement = (Element) parameterItor.next();
            String parameterName = parameterElement.attributeValue("name");
            if (parameterName.equals("datasource")) {
                dataSource = parameterElement.getStringValue();
            } else if (parameterName.equals("tableName")) {
                tableName = parameterElement.getStringValue();
            } else if (parameterName.equals("isClean")) {
                isClean = Boolean.parseBoolean(parameterElement.getStringValue());
            } else if (parameterName.equals("rowlimit")) {
                rowLimit = Integer.parseInt(parameterElement.getStringValue());
            }
        }

        parameterItor = operatorElt.element("parameterlist").elementIterator("parametermap");

        while (parameterItor.hasNext()) {
            Element paraMapElt = (Element) parameterItor.next();
            field2TableOutputSet.add(new Field2TableOutput(paraMapElt.attributeValue("streamfield"), paraMapElt.attributeValue("tablefield").toLowerCase()));
        }

        List<TableColumn> columnSet = null;
        synchronized (tableSet) {
            columnSet = tableSet.get(tableName);
            if (columnSet == null) {
                columnSet = MetaStoreProxy.getColumnList(dataSource, tableName);
                if (columnSet == null) {
                    throw new Exception("no table named " + tableName);
                }
                tableSet.put(tableName, columnSet);
            }
        }
        outputFormat = MetaStoreProxy.getOutputFormat(columnSet, field2TableOutputSet);
    }

    public static void main(String[] args) {
        File inputXml = new File("tableOutputOperator-specific.xml");
        try {
            String dataProcessDescriptor = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
                dataProcessDescriptor += line;
            }
            TableOutputOperator tableOutputOperator = new TableOutputOperator();
            tableOutputOperator.parseParameters(dataProcessDescriptor);
            tableOutputOperator.execute();
            System.out.println(tableOutputOperator.outputFormat);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

	@Override
	public void commit()
	{
	}
}
