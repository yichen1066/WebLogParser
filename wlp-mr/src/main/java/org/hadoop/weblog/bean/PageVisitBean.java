package org.hadoop.weblog.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: YICHEN
 * @Date: 2020/7/21 16:06
 */
public class PageVisitBean implements Writable {
    private String session;
    private String remoteAddr;
    private String inTime;
    private String outTime;
    private String inPage;
    private String outPage;
    private String referal;
    private Integer pageVisits;

    public void setProperties(String session, String remoteAddr, String inTime, String outTime, String inPage, String outPage, String referal, Integer pageVisits) {
        this.session = session;
        this.remoteAddr = remoteAddr;
        this.inTime = inTime;
        this.outTime = outTime;
        this.inPage = inPage;
        this.outPage = outPage;
        this.referal = referal;
        this.pageVisits = pageVisits;
    }

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getInTime() {
        return inTime;
    }

    public void setInTime(String inTime) {
        this.inTime = inTime;
    }

    public String getOutTime() {
        return outTime;
    }

    public void setOutTime(String outTime) {
        this.outTime = outTime;
    }

    public String getInPage() {
        return inPage;
    }

    public void setInPage(String inPage) {
        this.inPage = inPage;
    }

    public String getOutPage() {
        return outPage;
    }

    public void setOutPage(String outPage) {
        this.outPage = outPage;
    }

    public String getReferal() {
        return referal;
    }

    public void setReferal(String referal) {
        this.referal = referal;
    }

    public Integer getPageVisits() {
        return pageVisits;
    }

    public void setPageVisits(Integer pageVisits) {
        this.pageVisits = pageVisits;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(null == this.session ? "" : this.session);
        dataOutput.writeUTF(null == this.remoteAddr ? "" : this.remoteAddr);
        dataOutput.writeUTF(null == this.inTime ? "" : this.inTime);
        dataOutput.writeUTF(null == this.outTime ? "" : this.outTime);
        dataOutput.writeUTF(null == this.inPage ? "" : this.inPage);
        dataOutput.writeUTF(null == this.outPage ? "" : this.outPage);
        dataOutput.writeUTF(null == this.referal ? "" : this.referal);
        dataOutput.writeInt(this.pageVisits);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.session = dataInput.readUTF();
        this.remoteAddr = dataInput.readUTF();
        this.inTime = dataInput.readUTF();
        this.outTime = dataInput.readUTF();
        this.inPage = dataInput.readUTF();
        this.outPage = dataInput.readUTF();
        this.referal = dataInput.readUTF();
        this.pageVisits = dataInput.readInt();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.session);
        sb.append(",").append(this.getRemoteAddr());
        sb.append(",").append(this.getInTime());
        sb.append(",").append(this.getOutTime());
        sb.append(",").append(this.getInPage());
        sb.append(",").append(this.getOutPage());
        sb.append(",").append(this.getReferal());
        sb.append(",").append(this.getPageVisits());
        return sb.toString();
    }
}
