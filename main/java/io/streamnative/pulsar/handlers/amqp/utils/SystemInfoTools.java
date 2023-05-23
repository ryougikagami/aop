package io.streamnative.pulsar.handlers.amqp.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;

//import org.apache.logging.log4j.Logger;


public class SystemInfoTools {
    final static boolean isNotWindows = System.getProperties().getProperty("os.name").toLowerCase().indexOf("windows") < 0;
    final static BigDecimal DIVISOR = BigDecimal.valueOf(1024);
//    private final static Logger logger = Logger.getLogger(SystemInfoTools.class);

    public static int getPid(){
        return Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
    }

    public static MonitorInfoBean getMonitorInfoBean() {
        MonitorInfoBean monitorInfo = new MonitorInfoBean();
        if(!isNotWindows){
            monitorInfo.setMemUsage(500);
            return monitorInfo;
        }
        Runtime rt = Runtime.getRuntime();
        BufferedReader in = null;
        try {
            int pid = getPid();
            String[] cmd = {
                    "/bin/sh",
                    "-c",
                    "ps -aux | grep " + pid
            };
            Process p = rt.exec(cmd);
            in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            monitorInfo.setCpuUsage(in.readLine());
//            String str = null;
//            String[] strArray = null;
//            while ((str = in.readLine()) != null) {
//                logger.debug("top: " + str);
//                int m = 0;
//                strArray = str.split(" ");
//                for (int i = 0; i < strArray.length; i++) {
//                    String info = strArray[i];
//                    if (info.trim().length() == 0){
//                        continue;
//                    }
//                    if(m == 5) {//第5列为进程占用的物理内存值
//                        String unit = info.substring(info.length() - 1);
//                        if(unit.equalsIgnoreCase("g")) {
//                            monitorInfo.setMemUseSize(Double.parseDouble(info));
//                        } else if(unit.equalsIgnoreCase("m")) {
//                            BigDecimal memUseSize = new BigDecimal(info.substring(0, info.length() - 1));
//                            monitorInfo.setMemUseSize(memUseSize.divide(DIVISOR, 2, BigDecimal.ROUND_HALF_UP).doubleValue());
//                        } else {
//                            BigDecimal memUseSize = new BigDecimal(info).divide(DIVISOR);
//                            monitorInfo.setMemUseSize(memUseSize.divide(DIVISOR, 2, BigDecimal.ROUND_HALF_UP).doubleValue());
//                        }
//                    }
//                    if(m == 8) {//第9列为CPU的使用百分比
//                        monitorInfo.setCpuUsage(Double.parseDouble(info));
//                    }
//                    if(m == 9) {//第10列为内存的使用百分比
//                        monitorInfo.setMemUsage(Double.parseDouble(info));
//                    }
//                    m++;
//                }
//            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return monitorInfo;
    }

}