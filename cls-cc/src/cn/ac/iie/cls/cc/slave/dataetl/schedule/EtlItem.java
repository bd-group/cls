/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;


/**
 *
 * @author L-R
 * @version 0.5 reak-time update
 * @see  <IP:Port>|<TaskLen>|<UpdateTime>|<Speed>|<nextTaskTime>
 */
public class EtlItem
{
    private String ip;
    private int port;
    private int curTaskLen;
    private int speed;
    private long updateTime;
    private int nextTaskTime;

    public EtlItem(String ip, int port, int curTaskLen, int speed, long updateTime , int nextTaskTime) {
	this.ip = ip;
	this.port = port;
	this.curTaskLen = curTaskLen;
	this.speed = speed;
	this.updateTime=updateTime;
	this.nextTaskTime = nextTaskTime;

    }
    public EtlItem(){
	this("", 0, 0, 0, 0, 0);
    }
    public String getIp(){
	return ip;
    }
    public int getPort(){
	return port;
    }
    public int getCurTaskLength(){
	return curTaskLen;
    }
    public int getSpeed(){
	return speed;
    }
    public long getUpdateTime(){
	return updateTime;
    }
    public int getNextTaskTime(){
	return nextTaskTime;
    }
  
    public boolean setIp(String ip){
	this.ip=ip;
	return true;
    }
    public boolean setPort(int port){
	this.port=port;
	return true;
    }
    public boolean setCurTaskLength(int curTaskLen){
	this.curTaskLen=curTaskLen;
	return true;
    }
    public boolean setSpeed(int Speed){
	this.speed=Speed;
	return true;
    }
    public boolean setUpdateTime(long updateTime){
	this.updateTime=updateTime;
	return true;
    }
    public boolean setNextTaskTime(int nextTaskTime){
	this.nextTaskTime=nextTaskTime;
	return true;
    }

    @Override
    /*
     * @see 转化为zookeeper 中的形式
     */
    public String toString() {
	return ip+":"+port+"|"+curTaskLen+"|"+updateTime +"|"+speed +"|" +nextTaskTime;
    }

}
