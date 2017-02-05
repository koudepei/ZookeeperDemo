package com.dongnao.zookeeper.fluent;

/**
 * Created by koudepei on 2017/2/5.
 */
public class Student {

    private String name;

    private String phone;

    public Student setName(String name){
        this.name = name;
        return this;
    }
    public Student setPhone(String phone){
        this.phone=phone;
        return this;
    }

    public static Student build(){
        return new Student();
    }


    public String getName() {
        return name;
    }

    public String getPhone() {
        return phone;
    }
}
