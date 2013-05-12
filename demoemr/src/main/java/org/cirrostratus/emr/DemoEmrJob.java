package org.cirrostratus.emr;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.util.Arrays;

public class DemoEmrJob {


    public static void main(String[] argv) throws Exception {
        Log log = LogFactory.getLog(ReadZipToNameToFileMap.class);

        log.error("logged: " + Arrays.asList(argv));

        Options options = new Options();
        CommandLine commandLine = null;
        options.addOption(OptionBuilder.isRequired().withArgName("inputpath").hasArg().withDescription("Input path url glob (like, s3n://buckentname/path/*)").create("inputpath"));
        options.addOption(OptionBuilder.withArgName("outputpath").hasArg().withDescription("Output path (like, s3n://bucketname/path)").create("outputpath"));
        CommandLineParser clip = new GnuParser();
        try {
            commandLine = clip.parse(options, argv);
        } catch (ParseException e) {
            e.printStackTrace();
            throw e;
        }

        Configuration conf = new Configuration();

        Job job = new Job(conf, "DemoEmrJob");
        job.setJarByClass(DemoEmrJob.class);

        job.setMapperClass(ReadZipToNameToFileMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, new Path(commandLine.getOptionValue("inputpath")));
        FileOutputFormat.setOutputPath(job, new Path(commandLine.getOptionValue("outputpath")));

        int exitCode = job.waitForCompletion(true) ? 0 : 1;
    }
}
