package cn.ac.iie.cls.etl.dataprocess.dataset;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;

/**
 *
 * @author hanbing
 *
 */
public class IPField extends Field<IPField> {

    InetAddress inetAddress;

    public IPField(InetAddress fieldValue) {
        inetAddress = fieldValue;
        // TODO Auto-generated constructor stub
    }
    
   
    
    public String toString() {
        // TODO Auto-generated method stub
        if (inetAddress == null) {
            return null;
        } else {
            return inetAddress.getHostAddress();
        }
    }

    @Override
    public int compareTo(IPField anotherIpField) {
        String ipAddr = inetAddress.getHostAddress();
        String anoIpAddr = anotherIpField.inetAddress.getHostAddress();
        if ((inetAddress instanceof Inet4Address) && (anotherIpField.inetAddress instanceof Inet4Address)) {
            String[] ipv4 = ipAddr.split("//.");
            String[] anoipv4 = anoIpAddr.split("//.");
            for (int i = 0; i < 4; i++) {
                if (Integer.parseInt(ipv4[i]) > Integer.parseInt(anoipv4[i])) {
                    return 1;
                } else if (Integer.parseInt(ipv4[i]) < Integer.parseInt(anoipv4[i])) {
                    return -1;
                } else {
                    if (i == 3) {
                        return 0;
                    }
                    continue;
                }
            }
        } else if ((inetAddress instanceof Inet6Address) && (anotherIpField.inetAddress instanceof Inet6Address)) {
            String[] ipv6 = ipAddr.split(":");
            String[] anoipv6 = anoIpAddr.split(":");
            for (int i = 0; i < 8; i++) {
                if (Integer.parseInt(ipv6[i]) > Integer.parseInt(anoipv6[i])) {
                    return 1;
                } else if (Integer.parseInt(ipv6[i]) < Integer.parseInt(anoipv6[i])) {
                    return -1;
                } else {
                    if (i == 7) {
                        return 0;
                    }
                    continue;
                }
            }
        } else {
            System.out.println("can't compare ipv4 and ipv6");
        }
        return 2;
    }

    public InetAddress getInetAddress() {
        return inetAddress;
    }

    @Override
    public Object getFieldValue() {
        // TODO Auto-generated method stub
        return inetAddress;
    }
}
