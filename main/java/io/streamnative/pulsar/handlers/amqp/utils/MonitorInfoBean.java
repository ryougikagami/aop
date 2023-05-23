package io.streamnative.pulsar.handlers.amqp.utils;

public class MonitorInfoBean {

    /** cpu使用率 */
    private String cpuUsage;

    /** 内存使用率 */
    private double memUsage;

    /** 内存使用的大小 */
    private double memUseSize;

    public String getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(String cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public double getMemUsage() {
        return memUsage;
    }

    public void setMemUsage(double memUsage) {
        this.memUsage = memUsage;
    }

    public double getMemUseSize() {
        return memUseSize;
    }

    public void setMemUseSize(double memUseSize) {
        this.memUseSize = memUseSize;
    }

    @Override
    public String toString(){
        return "-------msg--------"+this.cpuUsage;
    }

}