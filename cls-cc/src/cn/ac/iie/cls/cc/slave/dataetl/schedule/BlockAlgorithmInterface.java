/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

import java.util.List;

/**
 *
 * @author L_R
 */
public interface BlockAlgorithmInterface {
    public boolean Schedule(List<TaskItem>taskList,List<EtlItem>etlList);
}
