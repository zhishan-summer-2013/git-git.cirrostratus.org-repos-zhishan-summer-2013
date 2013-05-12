package org.cirrostratus.emr;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: nkerr
 * Date: 2013-01-09
 * Time: 17:08
 * To change this template use File | Settings | File Templates.
 */
public class ReadZipToNameToFileMap extends Mapper<Object,Text,Text,BytesWritable> {
    Log log = LogFactory.getLog(ReadZipToNameToFileMap.class);
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        long start = System.currentTimeMillis();
        long end = -1;

        Path path = new Path(value.toString());
        log.error("made a path: " + path.toString());

        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FSDataInputStream fsin = fs.open(path);

        try {
            ArchiveInputStream astm = new ArchiveStreamFactory().createArchiveInputStream(fsin);

            ArchiveEntry ent = null;
            while ((ent = astm.getNextEntry()) != null) {
                if (ent.isDirectory()) {
                    continue;
                }
                String entryName = ent.getName();

                ByteArrayOutputStream baos = new ByteArrayOutputStream();

                IOUtils.copy(astm, baos);
                baos.close();
                context.write(new Text(value+"!"+entryName), new BytesWritable(baos.toByteArray()));
                context.getCounter(Counters.SUCCEEDED_READ_FILE_FROM_SUBMISSION).increment(1);
            }

        } catch (ArchiveException e) {
            end = System.currentTimeMillis();
            context.getCounter(Counters.TIME_IN_MAP_MILLIS).increment(end-start);
            context.getCounter(Counters.FAILED_SUBMISSION).increment(1);
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            log.error("Failed to unpack submission from s3 with execption: " + value, e);
            throw new IOException(e);
        }

        end = System.currentTimeMillis();
        context.getCounter(Counters.TIME_IN_MAP_MILLIS).increment(end-start);
        context.getCounter(Counters.SUCCEEDED_SUBMISSION).increment(1);
    }
}
