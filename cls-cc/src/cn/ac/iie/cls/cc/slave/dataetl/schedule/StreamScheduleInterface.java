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
public interface StreamScheduleInterface extends AlgorithmResetableInterface<StreamScheduleInterface, StreamAlgorithmInterface>,DestructorInterface{
    public TaskItem Schedule();
}
