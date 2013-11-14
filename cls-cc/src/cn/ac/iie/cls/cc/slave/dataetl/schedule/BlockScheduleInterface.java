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
public interface BlockScheduleInterface extends AlgorithmResetableInterface<BlockScheduleInterface,BlockAlgorithmInterface>,DestructorInterface{
    public boolean Schedule(List<TaskItem> taskList);
}
