package com.dongnao.zookeeper.fluent;

/**
 * Created by koudepei on 2017/2/5.
 */
public class FluentDemo {
    public static void main(String[] args) {
        //Fluent风格的代码编写操作方式
        Student stu=Student.build().setPhone("123").setName("root");
        System.out.println(stu.getName());
    }


}
