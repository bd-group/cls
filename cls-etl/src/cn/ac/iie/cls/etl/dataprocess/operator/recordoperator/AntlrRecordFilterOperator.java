/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.recordoperator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTask;
import cn.ac.iie.cls.etl.dataprocess.dataset.*;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;

/**
 *
 * @author alexmu, hanbing
 */
public class AntlrRecordFilterOperator extends Operator{
	
    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    private List<OutportFields> outportFields = new ArrayList<OutportFields>();
    private String expression = "";
    private boolean dropDataSet = false;
    private boolean outportAllColumn = false;
    
    private boolean dropErrorDataSet = false;

    CommonTree t = null;
    //Token tk = new CommonToken(Token.DOWN, "true");
	//Token tf = new CommonToken(Token.DOWN, "false");
	//CommonTree trueNode = new CommonTree(tk);
	//CommonTree falseNode = new CommonTree(tf);
	
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(RecordFilterOperator.class.getName());
    }

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    protected void init0() throws Exception {
    }

    public void validate() throws Exception {
    }

    public boolean getBoolResult(Tree t, Record record) throws Exception {
    	boolean flag = false;
    	if(t.getText().equals("EXP_BOOL")) {
    		flag =  judgeExpr(t.getChild(0).getText(), record.getField(t.getChild(0).getText()) , t.getChild(1).getText(), t.getChild(2).getText());
    	} else if(t.getText().equals("TOK_KW_OR")) {
			flag = false;
			for(int i=0; i<t.getChildCount(); i++) {
				if(getBoolResult(t.getChild(i), record)) {
					flag = true;
					break;
				}
			}
    	} else if (t.getText().equals("TOK_KW_AND")) {
			flag = true;
			for(int i=0; i<t.getChildCount(); i++) {
				if(!getBoolResult(t.getChild(i), record)) {
					flag = false;
					break;
				}
			}
		}
		return flag;
    }
    
    public boolean judgeExpr(String pFieldName, Object arg0, String relaChar, String arg1) throws Exception {
    	if(relaChar.equals("=")) {                 //handle '='
    		if (arg1.startsWith("'")) {
    			if(arg0 instanceof StringField) {
    				if (((StringField)arg0).toString().equals(arg1.substring(1, arg1.length()-1))) return true;
        			else return false;
    			} else {
    				throw new Exception("variable " + pFieldName + " is not a String type, please check your expression type");  			
    			}
    		} else if (arg1.toLowerCase().equals("null")) {
    			if (((Field)arg0).getFieldValue() == null) return true;
    			else return false;
    		} else if (arg1.toLowerCase().equals("true")) {
    			if (arg0 instanceof BooleanField) {
    				if (((BooleanField)arg0).getBoolean()) return true;
        			else return false;
    			} else {
    				throw new Exception("variable " + pFieldName + " is not a boolean type, please check your expression type");  			
    			}
    		} else if (arg1.toLowerCase().equals("false")) {
    			if (arg0 instanceof BooleanField) {
    				if (((BooleanField)arg0).getBoolean()) return false;
        			else return true;
    			} else {
    				throw new Exception("variable " + pFieldName + " is not a boolean type, please check your expression type");  			
    			}
    		} else {
    			if ((arg0 instanceof DoubleField) || (arg0 instanceof FloatField) || (arg0 instanceof IntegerField)) {
    				if (Double.compare(Double.parseDouble(((Field)arg0).toString()), Double.parseDouble(arg1)) == 0) return true;
        			else return false;
    			} else {
    				throw new Exception("variable " + pFieldName + " is not a Integer/Float/Double type, please check your expression type");  			
    			}
    		}
    	} else if (relaChar.equals(">")) {              //handle '>'
    		if ((arg0 instanceof DoubleField) || (arg0 instanceof FloatField) || (arg0 instanceof IntegerField)) {
				if (Double.parseDouble(((Field)arg0).toString())-Double.parseDouble(arg1) > 0.0001) return true;
	    		else return false;
			} else {
				throw new Exception("variable " + pFieldName + " is not a Integer/Float/Double type, please check your expression type");  			
			}
    	} else if (relaChar.equals("<")) {                 //handle '<'
    		if ((arg0 instanceof DoubleField) || (arg0 instanceof FloatField) || (arg0 instanceof IntegerField)) {
				if (Double.parseDouble(((Field)arg0).toString())-Double.parseDouble(arg1) < 0.0001) return true;
	    		else return false;
			} else {
				throw new Exception("variable " + pFieldName + " is not a Integer/Float/Double type, please check your expression type");  			
			}
    	} else if (relaChar.equals(">=")) {                //handle '>='
    		if ((arg0 instanceof DoubleField) || (arg0 instanceof FloatField) || (arg0 instanceof IntegerField)) {
				if (Double.parseDouble(((Field)arg0).toString())-Double.parseDouble(arg1) >= 0.0001) return true;
	    		else return false;
			} else {
				throw new Exception("variable " + pFieldName + " is not a Integer/Float/Double type, please check your expression type");  			
			}
    	} else if (relaChar.equals("<=")) {                //handle '<='
    		if ((arg0 instanceof DoubleField) || (arg0 instanceof FloatField) || (arg0 instanceof IntegerField)) {
				if (Double.parseDouble(((Field)arg0).toString())-Double.parseDouble(arg1) <= 0.0001) return true;
	    		else return false;
			} else {
				throw new Exception("variable " + pFieldName + " is not a Integer/Float/Double type, please check your expression type");  			
			}
    	} else if (relaChar.equals("!=") || relaChar.equals("^=") || relaChar.equals("<>")) {        //handle inequality sign
    		if (arg1.startsWith("'")) {
    			if (arg0 instanceof StringField) {
    				if (((StringField)arg0).toString().equals(arg1.substring(1, arg1.length()-1))) return false;
        			else return true;
    			} else {
    				throw new Exception("variable " + pFieldName + " is not a String type, please check your expression type");
    			}
    		} else if (arg1.toLowerCase().equals("null")) {
    			if (((Field)arg0).getFieldValue() == null) return false;
    			else return true;
    		} else if (arg1.toLowerCase().equals("true")) {
    			if (arg0 instanceof BooleanField) {
    				if (((BooleanField)arg0).getBoolean()) return false;
        			else return true;
    			} else {
    				throw new Exception("variable " + pFieldName + " is not a boolean type, please check your expression type");
    			}
    		} else if (arg1.toLowerCase().equals("false")) {
    			if (arg0 instanceof BooleanField) {
    				if (((BooleanField)arg0).getBoolean()) return true;
        			else return false;
    			} else {
    				throw new Exception("variable " + pFieldName + " is not a boolean type, please check your expression type");
    			}
    		} else {
    			if ((arg0 instanceof DoubleField) || (arg0 instanceof FloatField) || (arg0 instanceof IntegerField)) {
    				if (Double.compare(Double.parseDouble(((Field)arg0).toString()), Double.parseDouble(arg1)) == 0) return false;
        			else return true;
    			} else {
    				throw new Exception("variable " + pFieldName + " is not a Integer/Float/Double type, please check your expression type");  			
    			}
    		}
    	} else if (relaChar.equals("TOK_KW_LIKE")) {               //handle 'like'
    		if (arg0 instanceof StringField) {
    			String matchstr = ((StringField)arg0).getString();
    			String childstr = arg1.substring(1, arg1.length()-1);
    			
    			String regex = childstr.replaceAll("%", ".*").replaceAll("_", ".");
    			Pattern p = Pattern.compile(regex);
    			Matcher m = p.matcher(matchstr);
    			return m.find();
    		} else {
				throw new Exception("variable " + pFieldName + " is not a String type, please check your expression type");
    		}
    	}
    	return false;
    }
    
    
    @Override
	public void commit()
   	{
   	}
    
    @Override
	public void start()
   	{
   		// TODO Auto-generated method stub
   		synchronized (this)
   		{
   			notifyAll();
   		}
   	}
    
    protected void execute() {
        try {
        	while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();
    			DataSet matchedDataSet = dataSet.cloneDataSetWithMetadata();
    			DataSet unmatchedDataSet = dataSet.cloneDataSetWithMetadata();
                if (dataSet.isValid())
                {
                    boolean isMapped = false;
                    for (int i = 0; i < dataSet.size(); i++) 
                    {
                        Record record = dataSet.getRecord(i);
                        try {
                        	isMapped = getBoolResult(t, record);
                        } catch (Exception et) {
                        	status = FAILED;
                        	portSet.get(ERROR_PORT).write(dataSet);
                        	dropErrorDataSet = true;
                			logger.warn("there are errors in matching the expression", et);
                        	System.out.println("12312---错啦啦啦啦啦");
                        	break;
                        }
                        System.out.println("3---" + isMapped + ", " + i + ", " + dataSet.size() + ", " +dropDataSet);
                        if (isMapped) {
                        	matchedDataSet.appendRecord(record);
                        }
                        else {
                        	unmatchedDataSet.appendRecord(record);
                        }
                    }
                    System.out.println("----------------");
                    System.out.println("unmatchedsize = " + unmatchedDataSet.size());
                    System.out.println("matchsize = " + matchedDataSet.size());
            
                    if (!dropErrorDataSet) 
                    {
                    	if (matchedDataSet.size() > 0) {
                    		if (outportAllColumn) {
                                portSet.get(OUT_PORT).write(dataSet);
                            } else {
                            	List<String> fieldNames = dataSet.getFieldNameList();
                            	for (int i=0; i<outportFields.size(); i++) {
                            		fieldNames.remove(outportFields.get(i).fieldName);
                            	}
                            	System.out.println("fieldNamessize = " + fieldNames + ", " + fieldNames.size());
                            	for (int i=0; i<fieldNames.size(); i++) {
                            		System.out.println("field :" + fieldNames.get(i));
                            		matchedDataSet.removeField(fieldNames.get(i));
                            		System.out.println("match :" + matchedDataSet.getFieldNum());
                            	}
                            	portSet.get(OUT_PORT).write(matchedDataSet);
                            }
                    	}
                    	
                    	if (unmatchedDataSet.size() > 0) {
                    		portSet.get(ERROR_PORT).write(unmatchedDataSet);
                    	}
                    }
                    reportExecuteStatus();
                } else {
                    portSet.get(OUT_PORT).write(dataSet);
                    break;
                }
                dropErrorDataSet = false;
                break;
            }
            status = SUCCEEDED;
        } catch (Exception ex) {
        	status = FAILED;
            logger.warn("there are errors in executing the RecordFilter" + ex.getMessage(), ex);
        } finally {
        	/*synchronized(this){
        		try
    			{
    				wait();
    			} catch (InterruptedException e)
    			{
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
        	}*/
            try {
                portSet.get(OUT_PORT).write(DataSet.getDataSet(null, DataSet.EOS));
            } catch (Exception ex2) {
            	status = FAILED;
                logger.error("Writing DataSet.EOS failed for " + ex2.getMessage(), ex2);
            }
            reportExecuteStatus();
        }
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        logger.info("Start parsing");
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        
        //Element operatorElement = operatorElt.element("operator");
        String operatorName = operatorElt.attributeValue("name");
        
        Iterator parameterItor = operatorElt.elementIterator("parameter");
        
        if (!parameterItor.hasNext()) {
            status = FAILED;
            logger.error("operator " + operatorName + ": xml file lacks root element");
            throw new Exception("operator " + operatorName + ": xml file lacks root element");
        }
        
        while (parameterItor.hasNext()) {
            Element parameterElement = (Element) parameterItor.next();
            String parameterName = parameterElement.attributeValue("name");
            if (parameterName.equals("expression")) {
                expression = parameterElement.getStringValue();
                if (expression.isEmpty()) {
                	status = FAILED;
                	logger.warn("operator " + operatorName + ": expression is null");
                    throw new Exception("operator " + operatorName + ": expression code is null");
                }
            } else if (parameterName.equals("dropResultSet")) {
                dropDataSet = Boolean.parseBoolean(parameterElement.getStringValue());
            } else if (parameterName.equals("outportAllColumn")) {
                outportAllColumn = Boolean.parseBoolean(parameterElement.getStringValue());
            } else {
                logger.warn("wrong parameter configuration!");
            }
        }
        if (!outportAllColumn) {
            Element parameterListElt = operatorElt.element("parameterlist");
            Iterator parametermapItor = parameterListElt.elementIterator("parametermap");
            while (parametermapItor.hasNext()) {
                Element parametermapElt = (Element) parametermapItor.next();
                String fieldName = parametermapElt.attributeValue("fieldname");
                if(fieldName == null){
                	status = FAILED;
                	logger.warn("operator " + operatorName + ": fieldname is null");
                    throw new Exception("operator " + operatorName + ": fieldname is null");
                }
                outportFields.add(new OutportFields(fieldName));
            }
        }

        ANTLRStringStream input = new ANTLRStringStream(expression);
        FilterLexer lexer = new FilterLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        FilterParser parser = new FilterParser(tokens);
        FilterParser.program_return result = parser.program();
        t = (CommonTree)result.getTree();
        logger.info("Parsing  configuration file is successful");
    }

    class OutportFields {

        public String fieldName;

        public OutportFields(String pFieldName) {
            fieldName = pFieldName;
        }
    }
    
    class GrammerTree extends CommonTree implements Cloneable
    {
    	public GrammerTree clone() throws CloneNotSupportedException
    	{
    		return (GrammerTree)super.clone();
    	}
    }
    
    public static void main(String[] args){
    	File inputXml = new File("AntlrRecordFilterOperator-test-specific.xml");
        try {
            String dataProcessDescriptor = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
                dataProcessDescriptor += line;
            }
            ETLTask etlTask = ETLTask.getETLTask(dataProcessDescriptor);
            Thread etlTaskRunner = new Thread(etlTask);
            etlTaskRunner.start();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
    }
}
