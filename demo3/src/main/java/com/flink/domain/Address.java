package com.flink.domain;

import java.util.Date;

public class Address {
    private Integer id;
    private String province;
    private String city;

    public Address(Integer id, String province, String city) {
        this.id = id;
        this.province = province;
        this.city = city;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
