/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

/**
 *
 * @author L_R
 */
public interface AlgorithmResetableInterface<ReturnType , AlgorithmType> {
    public ReturnType setAlgorithm(AlgorithmType algorithm );
}
