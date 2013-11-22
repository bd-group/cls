/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTask;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author alexmu
 */
public abstract class Operator implements Runnable {

    protected String name;
    protected Operator parentOperator;
    protected Map<String, Port> portSet = new HashMap<String, Port>();
    protected ETLTask task = null;
    protected int status;
    protected boolean commit = false;
    public static final int STANDBY = 0;
    public static final int EXECUTING = 0;
    public static final int SUCCEEDED = 2;
    public static final int FAILED = -1;
    
    public volatile boolean abort = false;

    protected abstract void setupPorts() throws Exception;

    protected abstract void parseParameters(String pParameters) throws Exception;
    
    public void init(String pName, String pParameters) throws Exception {
        name = pName;
        status = STANDBY;
        init0();
        setupPorts();
        parseParameters(pParameters);
    }

    protected abstract void init0() throws Exception;

    public void setupPort(Port pPort) {
        portSet.put(pPort.getName(), pPort);
    }

    public Port getPort(String pPortName) {
        return portSet.get(pPortName);
    }

    public String getName() {
        return name;
    }
    
    public Operator getParentOperator(){
        return parentOperator;
    }

    public void setParentOperator(Operator parentOperator) {
        this.parentOperator = parentOperator;
    }

    public int getStatus()
	{
		return status;
	}

	public abstract void validate() throws Exception;

    public void setTaskManager(ETLTask pTaskManager) {
        task = pTaskManager;
    }

    public boolean isDone() {
        if (status == SUCCEEDED || status == FAILED) {
            return true;
        } else {
            System.out.println(name + "'status:" + status);
            return false;
        }
    }

    protected void reportExecuteStatus() {
        Map<String, Long> portMetrics = new HashMap<String, Long>();
        Iterator portIter = portSet.values().iterator();
        while (portIter.hasNext()) {
            Port port = (Port) portIter.next();
            portMetrics.put(port.getName(), port.getMetric());
        }
        task.report(this, portMetrics);
    }

    protected abstract void execute();
    public abstract void commit();
    public abstract void start();
    
    public void run() {
        System.out.println(name + " starts.");
        status = EXECUTING;
        execute();
        System.out.println(name + " exit.");
    }
}
