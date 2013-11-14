/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.domaintreesearch;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author root
 */
public class Node {

    String key;
    boolean meanful;
    Map<String, Node> subNodes;

    public Node(String _key, boolean _meanful) {
        this.key = _key;
        this.meanful = _meanful;
        this.subNodes = new HashMap<String, Node>();
    }

    public boolean isMeanful() {
        return meanful;
    }
}
