package com.hiwes.flink.Zinterview;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

public class JavaTest {

    public static void main(String[] args) {
        Integer a = 1;
        Integer b = 2;
        System.out.printf("a = %s, b = %s\n", a, b);
        swap(a, b);
        System.out.printf("a = %s, b = %s\n", a, b);

        System.out.println("----------------");
        short s1 = 1;
        short s2 = 1;
        System.out.println(s1 == s2);
        int i1 = 129;
        int i2 = 129;
        System.out.println(i1 == i2);
        Integer integer1 = 129;
        Integer integer2 = 129;
        System.out.println(integer1 == integer2);
        System.out.println(integer1.equals(integer2));
        System.out.println("----------------");
        String str1 = "str";
        String str2 = new String("str");
        String str3 = "str";
        System.out.println(str1 == str2);
        System.out.println(str1.equals(str2));
        System.out.println(str1 == str3);
        System.out.println(str1.equals(str3));
        System.out.println("----------------");

        int[][] arr = new int[3][3];
        arr[0][0] = 1;
        arr[0][1] = 2;
        arr[0][2] = 3;
        arr[1][0] = 4;
        arr[1][1] = 5;
        arr[1][2] = 6;
        arr[2][0] = 7;
        arr[2][1] = 8;
        arr[2][2] = 9;
        for (int[] s : arr) {
            for (int i : s) {
                System.out.println(i);
            }
        }
        System.out.println("----------------");
        new StaticInner().paint(); // 静态内部类

        JavaTest jt = new JavaTest(); // 创建内部类分2步
        JavaTest.Inner inner = jt.new Inner();
        inner.paint();

        System.out.println("----------------");
        try {
            // 使用Class类的newInstance方法
            Class javaTestClass = Class.forName("com.hiwes.flink.Zinterview.Hello");
            Hello h = (Hello) javaTestClass.newInstance();
            h.sayWorld();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("----------------");
        try {
            // 使用Constructor类的newInstance方法
            Class javaTestClass = Class.forName("com.hiwes.flink.Zinterview.Hello");
            Constructor con = javaTestClass.getConstructor();
            Hello h = (Hello) con.newInstance();
            h.sayWorld();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("----------------");

    }

    public static void swap(Integer a, Integer b) {
        int temp = a.intValue();
        try {
            Field value = Integer.class.getDeclaredField("value");
            value.setAccessible(true);
            value.set(a, b);
            value.set(b, new Integer(temp));
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    private static int number = 100;

    public static class StaticInner {
        private int number = 200;

        public void paint() {
            int number = 500;
            System.out.println(number);
            System.out.println(this.number);
            System.out.println(JavaTest.number);
        }
    }

    public class Inner {
        private int number = 200;

        public void paint() {
            int number = 500;
            System.out.println(number);
            System.out.println(this.number);
            System.out.println(JavaTest.number);
        }
    }

}
