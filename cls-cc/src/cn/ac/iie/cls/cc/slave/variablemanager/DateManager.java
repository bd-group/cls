package cn.ac.iie.cls.cc.slave.variablemanager;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateManager
{
	/*public enum VariableType{
		FILE_PATH, DATE
	}
	
	public static String repalceDateWithUDV(String content, String UDVTime, String pattern) {
		String regex = "#\\{DATE,\\s?(yyyy)?(-)?(MM|M)?(-)?(dd)?\\s?(HH?(:)?)?(mm(:)?)?(ss)?\\s?\\}";
		Date date = new Date();
		
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(content);
		while(m.find()) {
			String group = m.group();
			String datePattern = group.substring(7, group.length()-1).trim();
			System.out.println(datePattern);
			content = content.replace(group, (new SimpleDateFormat(datePattern)).format(date));
			System.out.println("1---" + group + ", " + content + ", " + (new SimpleDateFormat(datePattern)).format(date));
		}
		System.out.println("2---" + content);
		return content;
	}*/
	
	public static String repalceDateWithSysTime(String content) {
		String regex = "#\\{DATE,\\s?(yyyy)?(-)?(MM|M)?(-)?(dd)?\\s?(HH?(:)?)?(mm(:)?)?(ss)?\\s?\\}";
		Date date = new Date();
		
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(content);
		while(m.find()) {
			String group = m.group();
			String datePattern = group.substring(7, group.length()-1).trim();
			System.out.println(datePattern);
			content = content.replace(group, (new SimpleDateFormat(datePattern)).format(date));
			System.out.println("1---" + group + ", " + content + ", " + (new SimpleDateFormat(datePattern)).format(date));
		}
		System.out.println("2---" + content);
		return content;
	}
}
