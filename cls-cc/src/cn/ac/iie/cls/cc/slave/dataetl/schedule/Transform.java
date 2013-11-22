/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

/**
 *
 * @author L-R
 */
public class Transform {
    /*
     * @vision 0.5 real-time update
     * <IP:Port>|<TaskLen>|<UpdateTime>|<Speed>|<nextUpdateTime(delta)>
     * 地址 | 任务列表长度 | 最后更新时间 | 处理速度 | <已舍弃>
     */
    static public EtlItem Trans(String str)
    {
		String []dvd = str.split("\\|");
		String []addr = dvd[0].split(":");
		return new EtlItem(addr[0] , Integer.valueOf( addr[1]) , Integer.valueOf( dvd[1]) , Integer.valueOf(dvd[3]) , Long.valueOf(dvd[2]) , Integer.valueOf(dvd[4]) );
    }
}
