package com.zz.flink.rocketmq.loan;

public class LoanMsg {

    private String acctNo;

    private String oppositeAcctNo;

    private String oppositeAcctName;

    private String timestamp;

    public String getAcctNo() {
        return acctNo;
    }

    public void setAcctNo(String acctNo) {
        this.acctNo = acctNo;
    }

    public String getOppositeAcctNo() {
        return oppositeAcctNo;
    }

    public void setOppositeAcctNo(String oppositeAcctNo) {
        this.oppositeAcctNo = oppositeAcctNo;
    }

    public String getOppositeAcctName() {
        return oppositeAcctName;
    }

    public void setOppositeAcctName(String oppositeAcctName) {
        this.oppositeAcctName = oppositeAcctName;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
