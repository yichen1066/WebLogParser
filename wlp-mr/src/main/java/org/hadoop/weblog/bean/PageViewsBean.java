package org.hadoop.weblog.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PageViewsBean implements Writable {

	private String session;
	private String remoteAddr;
	private String timeStr;
	private String request;
	private int step;
	private String stayLong;
	private String referal;
	private String userAgent;
	private String bytesSend;
	private String status;

	public void set(String session, String remote_addr, String useragent, String timestr, String request, int step, String staylong, String referal, String bytes_send, String status) {
		this.session = session;
		this.remoteAddr = remote_addr;
		this.userAgent = useragent;
		this.timeStr = timestr;
		this.request = request;
		this.step = step;
		this.stayLong = staylong;
		this.referal = referal;
		this.bytesSend = bytes_send;
		this.status = status;
	}

	public String getSession() {
		return session;
	}

	public void setSession(String session) {
		this.session = session;
	}

	public String getRemote_addr() {
		return remoteAddr;
	}

	public void setRemote_addr(String remote_addr) {
		this.remoteAddr = remote_addr;
	}

	public String getTimestr() {
		return timeStr;
	}

	public void setTimestr(String timestr) {
		this.timeStr = timestr;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public int getStep() {
		return step;
	}

	public void setStep(int step) {
		this.step = step;
	}

	public String getStaylong() {
		return stayLong;
	}

	public void setStaylong(String staylong) {
		this.stayLong = staylong;
	}

	public String getReferal() {
		return referal;
	}

	public void setReferal(String referal) {
		this.referal = referal;
	}

	public String getUseragent() {
		return userAgent;
	}

	public void setUseragent(String useragent) {
		this.userAgent = useragent;
	}

	public String getBytes_send() {
		return bytesSend;
	}

	public void setBytes_send(String bytes_send) {
		this.bytesSend = bytes_send;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.session = in.readUTF();
		this.remoteAddr = in.readUTF();
		this.timeStr = in.readUTF();
		this.request = in.readUTF();
		this.step = in.readInt();
		this.stayLong = in.readUTF();
		this.referal = in.readUTF();
		this.userAgent = in.readUTF();
		this.bytesSend = in.readUTF();
		this.status = in.readUTF();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(session);
		out.writeUTF(remoteAddr);
		out.writeUTF(timeStr);
		out.writeUTF(request);
		out.writeInt(step);
		out.writeUTF(stayLong);
		out.writeUTF(referal);
		out.writeUTF(userAgent);
		out.writeUTF(bytesSend);
		out.writeUTF(status);

	}

}
