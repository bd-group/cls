/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.domaintreesearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author root
 */
public class SubDomainNameMatch {
    DomainNameTree dnTree = new DomainNameTree();
    Map<String, String> excludeStrSet = new HashMap<String, String>();

    public void init(List<String> _domainNameList, List<String> _excludeStrList) {
        for (int i = 0; i < _excludeStrList.size(); i++) {
            excludeStrSet.put(_excludeStrList.get(i), _excludeStrList.get(i));
        }

        Cell[] cells = null;
        for (int i = 0; i < _domainNameList.size(); i++) {
            cells = getCell(_domainNameList.get(i));
            dnTree.addNode(cells);
        }
    }

    public Cell[] getCell(String str) {

        String[] items = str.split("\\.");
        int totalLength = items.length;
        Cell[] cells = new Cell[items.length];
        String currItem = null;
        for (int i = 0; i < totalLength; i++) {
            currItem = items[totalLength - 1 - i];
            cells[i] = new Cell(currItem, !excludeStrSet.containsKey(currItem));
        }

        return cells;
    }

    public String getMainDomain(String domainStr) {
        return dnTree.getBranch(getCell(domainStr));
    }

    public static void main(String[] args) {
        List<String> domainNameList = new ArrayList<String>() {

            {
                add("www.google.com");
                add("www.people.com.cn");
            }
        };
        
        List<String> excludeStrList = new ArrayList<String>() {

            {
                add("com");
                add("cn");
            }
        };
        
        SubDomainNameMatch ssm = new SubDomainNameMatch();
        ssm.init(domainNameList, excludeStrList);
        
        System.out.println(ssm.getMainDomain("bbs.abc.people.com.cn"));

    }
}
