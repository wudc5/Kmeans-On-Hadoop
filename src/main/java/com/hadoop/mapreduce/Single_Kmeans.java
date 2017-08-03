package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 16-12-4.
 */
public class Single_Kmeans {

    // static List<ArrayList<Double>> centers ;
    // static int K;
    // static int dataBeginIndex;

    public static class KmeansMapper extends
            Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(",");

            List<ArrayList<Double>> centers = Help.getOldCenters(context
                    .getConfiguration().get("centersPath"));
            int dataBeginIndex = Integer.parseInt(context.getConfiguration()
                    .get("dtBegIdxPath"));
            int K = Integer.parseInt(context.getConfiguration().get("KPath"));

            double minDistance = 99999999;
            int centerIndex = K;
            for (int i = 0; i < K; i++) {
                double currentDistance = 0;
                for (int j = dataBeginIndex; j < fields.length; j++) {
                    double t1 = Math.abs(centers.get(i).get(j));
                    double t2 = Math.abs(Double.parseDouble(fields[j]));
//                    currentDistance += Math.pow((t1 - t2) / (t1 + t2), 2);
                    currentDistance += Math.pow((t1 - t2), 2);
                }
                Help.debug(currentDistance, "currentDistance");
                if (minDistance > currentDistance) {
                    minDistance = currentDistance;
                    centerIndex = i;
                }
            }
            IntWritable centerId = new IntWritable(centerIndex+1);
            Text tValue = new Text();
            tValue.set(value);
            context.write(centerId, tValue);
        }
    }

    public static class KmeansReducer extends
            Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            List<ArrayList<Double>> helpList = new ArrayList<ArrayList<Double>>();
            String tmpResult = "";
            for (Text val : values) {
                String line = val.toString();
                String[] fields = line.split(",");
                List<Double> tmpList = new ArrayList<Double>();
                for (int i = 0; i < fields.length; i++) {
                    tmpList.add(Double.parseDouble(fields[i]));
                }
                helpList.add((ArrayList<Double>) tmpList);
            }

            // System.out.println(helpList.size());
            // for(int i=0;i<helpList.size();i++)
            // System.out.println(helpList.get(i));

            for (int i = 0; i < helpList.get(0).size(); i++) {
                double sum = 0;
                for (int j = 0; j < helpList.size(); j++) {
                    sum += helpList.get(j).get(i);
                }
                double t = sum / helpList.size();
                if (i == 0)
                    tmpResult += t;
                else
                    tmpResult += "," + t;
            }
            Text result = new Text();
            result.set(tmpResult);
            int tmpKey = Integer.parseInt(key.toString());
            context.write(new IntWritable(tmpKey), result);
        }
    }

    static void runKmeans(String[] args, boolean isReduce) throws IOException,
            ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 7) {
            System.err
                    .println("Usage: Kmeans <in> <out> <localOriginalCentersPath> <oldCentersPath> <newCentersPath> <dataBeginIndex> <K>");
            System.exit(2);
        }

        conf.setStrings("centersPath", otherArgs[3]);
        conf.setStrings("dtBegIdxPath", otherArgs[5]);
        conf.setStrings("KPath", otherArgs[6]);

        Job job = new Job(conf, "kmeans");
        job.setJarByClass(Kmeans.class);
        job.setMapperClass(KmeansMapper.class);
        job.setNumReduceTasks(Integer.parseInt(args[6]));
        // 判断是否需要执行Reduce
        if (isReduce) {
            job.setReducerClass(KmeansReducer.class);
        }
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // delete last result  (hdfs://localhost:9000/user/hadoop/Kmeans/out)
        Help.deleteLastResult(otherArgs[1]);

        // System.exit(job.waitForCompletion(true)?0:1);
        job.waitForCompletion(true);

    }

    /**
     *
     * @param in
     *            - args[0] out - args[1] 	localOriginalCentersPath - args[2]
     *            oldCentersPath - args[3] 	newCentersPath - args[4]
     *            dataBeginIndex - args[5] 	K - args[6]
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        System.out.println("args[0]: " + args[0]);
        System.out.println("args[1]: " + args[1]);
        System.out.println("args[2]: " + args[2]);
        System.out.println("args[3]: " + args[3]);
        System.out.println("args[4]: " + args[4]);
        System.out.println("args[5]: " + args[5]);
        System.out.println("args[6]: " + args[6]);
        Help.deleteLastResult(args[3]);
        Help.copyOriginalCenters(args[2], args[3]);

        int count=1;
//		runKmeans(args, true);

        while (true) {
//            if(count == 5){
//                break;
//            }
            System.out.println("迭代的轮次： "+count++);
            runKmeans(args, true);
            if (Help.isFinished(args[3], args[4], args[6], args[5], 2000)) {
                runKmeans(args, false);
                break;
            }
        }
    }
}
        /*
        args[0]: hdfs://localhost:9000/user/hadoop/Kmeans/input
        args[1]: hdfs://localhost:9000/user/hadoop/Kmeans/out
        args[2]: /home/hadoop/oldCenters.data
        args[3]: hdfs://localhost:9000/user/hadoop/Kmeans/oldCenters.data
        args[4]: hdfs://localhost:9000/user/hadoop/Kmeans/out/part-r-0000
        args[5]: 1
        args[6]: 3
        */
