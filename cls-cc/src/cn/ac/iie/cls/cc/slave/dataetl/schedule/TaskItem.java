/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

import javax.print.DocFlavor;

/**
 *
 * @author L-R
 */
public class TaskItem
{
    String Id;
    String filePath;
    int length;
    /*
     * @version = 0.5
     */
    String etlIP;
    int etlPort;
    /*
     * @version = 0.6
     */
    int importance;//max=5 ,min=1
    /*
     * @version = 1.0
     */
    int hardLevel;//min=1 ,etl.curLength = Sigma(task[i].length * task[i].hardLeval)

    public TaskItem() {
	this("", "", 0);
    }

    public TaskItem(String Id, String filePath, int length) {
	this(Id, filePath, length, "", 0);
    }

    /*
     * @version = 0.5
     */
    public TaskItem(String Id, String filePath, int length, String etlIP, int etlPort) {
	this(Id, filePath, length, etlIP, etlPort, 1);
    }
    
    public TaskItem(String etlIP , int etlPort){
	this("", "", 0, etlIP, etlPort, 1);
    }

    /*
     * @version = 0.6
     */
    public TaskItem(String Id, String filePath, int length, String etlIP, int etlPort, int importance) {
	this(Id, filePath, length, etlIP, etlPort, importance, 1);
    }

    /*
     * @version = 1.0
     */
    public TaskItem(String Id, String filePath, int length, String etlIP, int etlPort, int importance, int hardLevel) {
	this.Id = Id;
	this.filePath = filePath;
	this.length = length;
	this.etlIP = etlIP;
	this.etlPort = etlPort;
	this.importance = importance;
	this.hardLevel = hardLevel;
    }
    
    
    
    public void setEtlIp(String ip){
	this.etlIP = ip;
    }
    public void setEtlPort(int port){
	this.etlPort=port;
    }

    @Override
    public String toString() {
	
	return "ID:"+Id+"\tfilepath:"+filePath+"\tlength:"+length+"\tIP:"+etlIP+"\tPort:"+etlPort;
    }
    
    public String getId(){
	return  Id;
    }
    public String getFilePath(){
	return filePath;
    }
    public int getTaskLength(){
	return length;
    }
    public String getEtlIp(){
	return etlIP;
    }
    public int getEtlPort(){
	return etlPort;
    }
}
