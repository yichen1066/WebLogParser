package org.hadoop.weblog.bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: YICHEN
 * @Date: 2020/7/17 15:21
 */
public class WebLogBean implements Writable {

    private boolean valid = true;// 判断数据是否合法
    private String remoteAddr;// 记录客户端的ip地址
    private String remoteUser;// 记录客户端用户名称,忽略属性"-"
    private String timeLocal;// 记录访问时间与时区
    private String request;// 记录请求的url与http协议
    private String status;// 记录请求状态；成功是200
    private String bodyBytesSent;// 记录发送给客户端文件主体内容大小
    private String httpReferer;// 用来记录从那个页面链接访问过来的
    private String httpUserAgent;// 记录客户浏览器的相关信息

    public void set(boolean valid, String remoteAddr, String remoteUser, String timeLocal, String request, String status, String bodyBytesSent, String httpReferer, String httpUserAgent) {
        this.valid = valid;
        this.remoteAddr = remoteAddr;
        this.remoteUser = remoteUser;
        this.timeLocal = timeLocal;
        this.request = request;
        this.status = status;
        this.bodyBytesSent = bodyBytesSent;
        this.httpReferer = httpReferer;
        this.httpUserAgent = httpUserAgent;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(this.valid);
        dataOutput.writeUTF(null == this.remoteAddr ? "" : this.remoteAddr);
        dataOutput.writeUTF(null == this.remoteUser ? "" : this.remoteUser);
        dataOutput.writeUTF(null == this.timeLocal ? "" : this.timeLocal);
        dataOutput.writeUTF(null == this.request ? "" : this.request);
        dataOutput.writeUTF(null == this.status ? "" : this.status);
        dataOutput.writeUTF(null == this.bodyBytesSent ? "" : this.bodyBytesSent);
        dataOutput.writeUTF(null == this.httpReferer ? "" : this.httpReferer);
        dataOutput.writeUTF(null == this.httpUserAgent ? "" : this.httpUserAgent);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.valid = dataInput.readBoolean();
        this.remoteAddr = dataInput.readUTF();
        this.remoteUser = dataInput.readUTF();
        this.timeLocal = dataInput.readUTF();
        this.request = dataInput.readUTF();
        this.status = dataInput.readUTF();
        this.bodyBytesSent = dataInput.readUTF();
        this.httpReferer = dataInput.readUTF();
        this.httpUserAgent = dataInput.readUTF();
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public void setRemoteUser(String remoteUser) {
        this.remoteUser = remoteUser;
    }

    public String getTimeLocal() {
        return timeLocal;
    }

    public void setTimeLocal(String timeLocal) {
        this.timeLocal = timeLocal;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBodyBytesSent() {
        return bodyBytesSent;
    }

    public void setBodyBytesSent(String bodyBytesSent) {
        this.bodyBytesSent = bodyBytesSent;
    }

    public String getHttpReferer() {
        return httpReferer;
    }

    public void setHttpReferer(String httpReferer) {
        this.httpReferer = httpReferer;
    }

    public String getHttpUserAgent() {
        return httpUserAgent;
    }

    public void setHttpUserAgent(String httpUserAgent) {
        this.httpUserAgent = httpUserAgent;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.valid);
        sb.append(",").append(this.getRemoteAddr());
        sb.append(",").append(this.getRemoteUser());
        sb.append(",").append(this.getTimeLocal());
        sb.append(",").append(this.getRequest());
        sb.append(",").append(this.getStatus());
        sb.append(",").append(this.getBodyBytesSent());
        sb.append(",").append(this.getHttpReferer());
        sb.append(",").append(this.getHttpUserAgent());
        return sb.toString();
    }
}
