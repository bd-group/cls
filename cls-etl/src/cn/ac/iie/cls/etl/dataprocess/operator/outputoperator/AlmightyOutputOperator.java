/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.outputoperator;

import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class AlmightyOutputOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(GlobalTableOuputOperator.class.getName());
    }

    @Override
    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    @Override
    protected void init0() throws Exception {
    }

    @Override
    public void validate() throws Exception {
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
    
    @Override
    protected void execute() {
        
        try {
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();                
                if (dataSet.isValid()) {
                    portSet.get(OUT_PORT).incMetric(dataSet.size());
                    int dataSize = dataSet.size();
                    int fieldSize = dataSet.getFieldNum();
                    //System.out.println("1---" + dataSize + ", " + dataSet.getFieldNameList().size() + ", " + dataSet.getAllRecords().size() + ", " + dataSet.getRecord(0).size());
                    System.out.println(dataSet.getFieldNameList());                    
                    for (int i = 0; i < dataSize; i++) {
                        Record record = dataSet.getRecord(i);
                        for (int j = 0; j < dataSet.getFieldNum(); j++) {
                            System.out.println(j == 0 ? record.getField(j) : "," + record.getField(j));
                        }
                        System.out.println();
                    }
                    System.out.println("output " + dataSet.size() + " records");
                } else {                   
                     break;
                }
            }
            status = SUCCEEDED;
        } catch (Exception ex) {
            status = FAILED;
            ex.printStackTrace();            
        } finally {
        	reportExecuteStatus();
        }
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
    }

	@Override
	public void commit()
	{
		// TODO Auto-generated method stub
		
	}
}
