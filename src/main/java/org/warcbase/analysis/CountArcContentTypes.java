package org.warcbase.analysis;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.jwat.arc.ArcRecordBase;

public class CountArcContentTypes {
  private static final Logger LOG = Logger.getLogger(CountArcContentTypes.class);

  private static enum Records { TOTAL };

  public static class MyMapper
      extends Mapper<LongWritable, ArcRecordBase, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    @Override
    public void map(LongWritable key, ArcRecordBase record, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);

      String type = record.getContentTypeStr();
      context.write(new Text(type), ONE);
    }
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + CountArcContentTypes.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    Tool tool = new ArcCounter(MyMapper.class);
    ToolRunner.run(tool, args);
  }
}
