/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.domaintreesearch;

/**
 *
 * @author root
 */
public class DomainNameTree {

    private Node rootNode;

    public DomainNameTree() {
        rootNode = new Node("", false);
    }

    public void addNode(Cell[] _cells) {
        Node parentNode = this.rootNode;
        Node subNode = null;
        String currKey = null;
        for (int i = 0; i < _cells.length; i++) {
            currKey = _cells[i].value;
            subNode = parentNode.subNodes.get(currKey);
            if (subNode == null) {
                subNode = new Node(currKey, _cells[i].meanful);
                parentNode.subNodes.put(currKey, subNode);
            }
            parentNode = subNode;
        }
    }

    public boolean isMatch(Cell[] _cells) {
        Node parentNode = this.rootNode;
        Node subNode = null;
        for (int i = 0; i < _cells.length; i++) {
            subNode = parentNode.subNodes.get(_cells[i].value);
            if (subNode != null) {
                if (subNode.isMeanful()) {
                    return true;
                } else {
                    parentNode = subNode;
                }
            } else {
                return false;
            }
        }
        return false;
    }

    public String getBranch(Cell[] _cells) {
        String v = "";
        Node parentNode = this.rootNode;
        Node subNode = null;
        for (int i = 0; i < _cells.length; i++) {
            subNode = parentNode.subNodes.get(_cells[i].value);
            if (subNode != null) {
                if (v.isEmpty()) {
                    if (!parentNode.key.isEmpty()) {
                        v = parentNode.key;
                    }
                } else {
                    v = parentNode.key + "." + v;
                }

                if (subNode.isMeanful()) {
                    while (true) {
                        v = subNode.key + "." + v;
                        if (subNode.subNodes.isEmpty()) {
                            return v;
                        } else {
                            String key = subNode.subNodes.keySet().iterator().next();
                            subNode = subNode.subNodes.get(key);
                        }
                    }
                } else {
                    parentNode = subNode;
                }
            } else {
                return "";
            }
        }
        return "";
    }
}
