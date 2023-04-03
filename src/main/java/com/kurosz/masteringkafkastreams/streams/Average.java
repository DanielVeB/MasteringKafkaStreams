package com.kurosz.masteringkafkastreams.streams;

public class Average {
    Average() {
        num = 0;
        sum = 0;
    }

    private int num;
    private int sum;

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }
}