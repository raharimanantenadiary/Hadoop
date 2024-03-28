package com.mycompany.wordcount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;

public class HadoopAnagram {

    public static enum MyCounters {
        COLOR_MAX
    }

    public static class MyMapper extends Mapper<Object, Text, IntWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            int nodeId = Integer.parseInt(parts[0]);
            String[] nodeInfo = parts[1].split("\\|");
            String nodeColor = nodeInfo[1];
            String[] children = nodeInfo[0].isEmpty() ? new String[0] : nodeInfo[0].split(",");
            int nodeDepth = Integer.parseInt(nodeInfo[2]);

            if (nodeColor.equals("GRIS")) {
                for (String child : children) {
                    context.write(new IntWritable(Integer.parseInt(child)), new Text(nodeId + "|GRIS|" + (nodeDepth + 1)));
                }
                context.write(new IntWritable(nodeId), new Text(String.join(",", children) + "|NOIR|" + (nodeDepth + 1)));
                // Mettre à jour la couleur maximale
                Counter colorMaxCounter = context.getCounter(MyCounters.COLOR_MAX);
                colorMaxCounter.increment(1);
            } else {
                context.write(new IntWritable(nodeId), new Text(String.join(",", children) + "|" + nodeColor + "|" + nodeDepth));
            }
        }
    }

    public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxDepth = -1;
            String maxColor = "BLANC";
            String maxChildren = "";

            for (Text value : values) {
                String[] parts = value.toString().split("\\|");
                String color = parts[1];
                int depth = Integer.parseInt(parts[2]);
                if (depth > maxDepth) {
                    maxDepth = depth;
                }
                if (color.compareTo(maxColor) > 0) {
                    maxColor = color;
                }
                if (parts[0].length() > maxChildren.length()) {
                    maxChildren = parts[0];
                }
            }

            context.write(key, new Text(maxChildren + "|" + maxColor + "|" + maxDepth));
        }
    }

   public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "My MapReduce");
    job.setJarByClass(HadoopAnagram.class);
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);

    boolean jobCompleted;
    long colorMax;
    do {
        // Définir les chemins d'entrée et de sortie
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Exécutez le travail et vérifiez si la condition de sortie est remplie
        jobCompleted = job.waitForCompletion(true);
        if (!jobCompleted) {
            System.exit(1);
        }

        // Vérifiez la couleur maximale
        Counter colorMaxCounter = job.getCounters().findCounter(MyCounters.COLOR_MAX);
        colorMax = colorMaxCounter.getValue();

        // Utilisez le résultat de sortie comme entrée pour la prochaine itération
        inputPath = outputPath;
        outputPath = new Path(args[1] + "_temp"); // Chemin temporaire pour la nouvelle sortie
        job = Job.getInstance(conf, "My MapReduce"); // Créez un nouveau travail pour la prochaine itération
    } while (colorMax != 0);

    System.exit(0);
}
}
