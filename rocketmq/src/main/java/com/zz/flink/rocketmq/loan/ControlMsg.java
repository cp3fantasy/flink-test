package com.zz.flink.rocketmq.loan;

public class ControlMsg {

    private String operateType;

    private String customerName;

    private String accountNo;

    public String getOperateType() {
        return operateType;
    }

    public void setOperateType(String operateType) {
        this.operateType = operateType;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public String getAccountNo() {
        return accountNo;
    }

    public void setAccountNo(String accountNo) {
        this.accountNo = accountNo;
    }

    @Override
    public String toString() {
        return "ControlMsg{" +
                "operateType='" + operateType + '\'' +
                ", customerName='" + customerName + '\'' +
                ", accountNo='" + accountNo + '\'' +
                '}';
    }
}
