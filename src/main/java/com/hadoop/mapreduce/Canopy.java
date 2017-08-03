package com.hadoop.mapreduce;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import java.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import static java.lang.System.*;
/**
 * Created by hadoop on 16-12-14.
 */
public class Canopy {

    public static double getDistance(String point1, String point2){
        double distance = 0;
        String [] line1 = point1.split(",");
        String [] line2 = point2.split(",");
        for (int i= 0; i<line1.length; i++){
            double t1 = Math.abs(Double.parseDouble(line1[i]));
            double t2 = Math.abs(Double.parseDouble(line2[i]));
            distance += Math.pow((t1 - t2), 2);
        }
        distance = Math.sqrt(distance);
        return distance;
    }
    public static double getAverDistance(List<String> dataList){
        double sum = 0;
        double averDis = 0;
        for (int i=0; i<dataList.size(); i++){
            for (int j=0; j<dataList.size(); j++){
                if(i == j)
                    continue;
                double dis = getDistance(dataList.get(i), dataList.get(j));
                sum += dis;
            }
        }
        averDis = sum / (dataList.size() * (dataList.size() - 1));
        return averDis;
    }
    public static void main(String []args) throws IOException, InterruptedException {
        String path = "/home/hadoop/feature.txt";

        Map map = new HashMap();
        List<String> dataList = Help.readTxtFile(path);

        double T2 = getAverDistance(dataList)/2;
        System.out.println("T2: "+T2);
        while (dataList.size() != 0){
            int size = dataList.size();
            int index = (int)(Math.random() * size);
            String center = dataList.get(index);
            dataList.remove(index);
            List<String> pointList = new ArrayList<String>();
            for(int i = 0; i<dataList.size(); i++){
                String point = dataList.get(i);
                double distance = getDistance(point, center);
                if (distance < T2){
                    pointList.add(point);
                    dataList.remove(i);
                }
            }
            map.put(center, pointList);
        }
        String saveFilePath = "/home/hadoop/javaResult.txt";
        Help.deleteFile(saveFilePath);      //删除历史结果
        for (Object key: map.keySet()){
            System.out.println("key: " + key);
            Help.writeTxtFile(saveFilePath, key.toString());
            System.out.println(map.get(key));
        }
        System.out.println("map'size: " + map.size());
        System.out.println("end.");


//        PythonInterpreter pyinter = new PythonInterpreter();
//        pyinter.execfile("/home/hadoop/hello.py");

//        Process proc = Runtime.getRuntime().exec("python  /home/hadoop/hello.py");
//        proc.waitFor();

    }
}
