package com.hadoop.mapreduce;

/**
 * Created by hadoop on 16-12-4.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

class Help {

    static final boolean DEBUG = false;

    public static void debug(Object o, String s) {
        if (DEBUG) {
            System.out.println(s + ":" + o.toString());
        }
    }

    public static List<ArrayList<Double>> getOldCenters(String inputPath) {
        List<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>();
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path inPath = new Path(inputPath);
            FSDataInputStream fsIn = hdfs.open(inPath);
            LineReader lineIn = new LineReader(fsIn, conf);
            Text line = new Text();
            while (lineIn.readLine(line) > 0) {

                String record = line.toString();
                String[] fields = record.split(",");
                List<Double> tmpList = new ArrayList<Double>();
                for (int i = 0; i < fields.length; i++)
                    tmpList.add(Double.parseDouble(fields[i]));
                result.add((ArrayList<Double>) tmpList);
            }
            fsIn.close();
        } catch (IOException e) {

            e.printStackTrace();
        }

        return result;
    }

    public static void deleteLastResult(String path) {
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path inPath = new Path(path);
            hdfs.delete(inPath);
        } catch (IOException e) {
        }
    }
    public static void deleteFile(String filepath){
        File file = new File(filepath);
        if(file.exists()){
            file.delete();
        }
    }

    public static void copyOriginalCenters(String src, String dst) {
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            System.out.println("src: "+src);
            System.out.println("dst: "+dst);
            hdfs.copyFromLocalFile(new Path(src), new Path(dst));
//            hdfs.copyFromLocalFile(new Path(dst), new Path(src));


        } catch (IOException e) {

        }
    }

    public static boolean isFinished(String oldPath, String newPath,
                                     String KPath, String dtBegIdxPath, double threshold)
            throws IOException {

        int dataBeginIndex = Integer.parseInt(dtBegIdxPath);
        int K = Integer.parseInt(KPath);
        List<ArrayList<Double>> oldCenters = Help.getOldCenters(oldPath);
        List<ArrayList<Double>> newCenters = new ArrayList<ArrayList<Double>>();
        System.out.println("oldcenters: "+oldCenters);
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        int num = 0;
        for (int t = 0; t < K; t++) {
            String newPath1;
            if(t >= 10){
                newPath1 = newPath.substring(0, newPath.length()-1);
            }else{
                newPath1 = newPath;
            }
            Path inPath = new Path(newPath1 + t);
            if (!hdfs.exists(inPath))
                break;
            FSDataInputStream fsIn = hdfs.open(inPath);
            LineReader lineIn = new LineReader(fsIn, conf);
            Text line = new Text();
            while (lineIn.readLine(line) > 0) {
                String tmp = line.toString();
                Help.debug("tmp", tmp);

                if(tmp.length()<5)//处理在集群上出现的key与value不在一行的情况
                {
                    System.out.println("tmp.length: "+tmp.length());
                    lineIn.readLine(line);
                    tmp = line.toString();
                    String []fields = tmp.split(",");
                    List<Double> tmpList = new ArrayList<Double>();
                    for (int i = 0; i < fields.length; i++)
                        tmpList.add(Double.parseDouble(fields[i]));
                    System.out.println("tmpList: "+tmpList);
                    newCenters.add((ArrayList<Double>) tmpList);
                    continue;
                }

                String[] tmpLine = tmp.split("	");
                Help.debug(tmpLine[1].toString(), tmpLine.toString());
                String record = tmpLine[1];  // 原来为tmpLine[1]
                String[] fields = record.split(",");
                List<Double> tmpList = new ArrayList<Double>();
                for (int i = 0; i < fields.length; i++)
                    tmpList.add(Double.parseDouble(fields[i]));
                newCenters.add((ArrayList<Double>) tmpList);
            }
            fsIn.close();
        }
        System.out.println("newCenter: "+ newCenters);
        System.out.println("oldCenter size:"+oldCenters.size()+"\nnewCenters size:"+newCenters.size());

        double distance = 0;
        for (int i = 0; i < K; i++) {
            for (int j = dataBeginIndex; j < oldCenters.get(0).size(); j++) {
                double t1 = Math.abs(oldCenters.get(i).get(j));
                double t2 = Math.abs(newCenters.get(i).get(j));
//                distance += Math.pow((t1 - t2) / (t1 + t2), 2);
                distance += Math.pow((t1 - t2), 2);
            }
        }
        distance = Math.sqrt(distance);      //欧氏距离
        System.out.println("distance: " + distance);
        if (distance <= threshold) {
            return true;
        }

        Help.deleteLastResult(oldPath);
        FSDataOutputStream os = hdfs.create(new Path(oldPath));

        for (int i = 0; i < newCenters.size(); i++) {
            String text = "";
            for (int j = 0; j < newCenters.get(i).size(); j++) {
                if (j == 0)
                    text += newCenters.get(i).get(j);
                else
                    text += "," + newCenters.get(i).get(j);
            }
            text += "\n";
            os.write(text.getBytes(), 0, text.length());
        }
        os.close();
        // ///////////////////////////
        return false;
    }
    public static boolean createFile(File fileName)throws Exception{
        boolean flag=false;
        try{
            if(!fileName.exists()){
                fileName.createNewFile();
                flag=true;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return true;
    }
    public static List<String> readTxtFile(String filePath){
        List<String> contentList = new ArrayList<String>();
        try {

            String encoding="GBK";
            File file=new File(filePath);
            if(file.isFile() && file.exists()){ //判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file),encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while((lineTxt = bufferedReader.readLine()) != null){
                    contentList.add(lineTxt);
                }
                read.close();
            }else{
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }
        return contentList;
    }


    public static void writeTxtFile(String file, String content) {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file, true)));
            out.write(content+"\n");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String [] args) throws Exception {
//        String filepath = "/home/hadoop/oldCenters.txt";
//        List<String> fileContent = readTxtFile(filepath);
//
//        System.out.printf(fileContent.toString());
//        String saveFilePath = "/home/hadoop/hahahahahah.txt";
//        writeTxtFile(saveFilePath, fileContent.toString());
//        writeTxtFile(saveFilePath, "sdfjslkdflsjkdflsdjf");
    }
}