package com.home.hadoop.distcp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import java.util.ArrayList;
import java.util.List;

public class DistcpToS3 {
    static String sourceStagingUri = "hdfs://127.0.0.1:9000/test.txt";
    static String targetStagingUri = "s3a://<bucket-name>";

    public static void main(String[] args) throws Exception {
        distcp();
    }

    private static void distcp() throws Exception {
        System.out.println("Starting Distcp to S3");
        DistCpOptions options = getDistCpOptions();
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        conf.set("fs.s3a.access.key", "<accesskey>");
        conf.set("fs.s3a.secret.key", "<secretKey>");

        DistCp distCp = new DistCp(conf, options);

        System.out.println("Started DistCp with source Path: "+sourceStagingUri+" \ttarget path: "+targetStagingUri);

        Job distcpJob = distCp.execute();
        distcpJob.waitForCompletion(true);
        System.out.println("Done");
    }


    public static DistCpOptions getDistCpOptions() {
        // DistCpOptions expects the first argument to be a file OR a list of Paths
        List<Path> sourceUris=new ArrayList();
        sourceUris.add(new Path(sourceStagingUri));
        DistCpOptions distcpOptions = new DistCpOptions(sourceUris, new Path(targetStagingUri));

        // setSyncFolder(true) ensures directory structure is maintained when source is copied to target
        distcpOptions.setSyncFolder(true);

        distcpOptions.setBlocking(true);
        return distcpOptions;
    }
}
