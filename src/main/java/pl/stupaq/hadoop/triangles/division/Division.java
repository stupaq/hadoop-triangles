package pl.stupaq.hadoop.triangles.division;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pl.stupaq.hadoop.triangles.TriplesPartitioner;
import pl.stupaq.hadoop.triangles.Tuple;

import java.util.Arrays;

// Partition vertices into groups and create reducer for each 3-element subset of groups,
// communication cost Omeg(|E| * 3/2 * buckets)
public class Division implements Tool {
  static final String BUCKETS_KEY = "triangles.division.buckets";
  private Configuration conf;

  public static void main(String[] args) throws Exception {
    try {
      Configuration conf = new Configuration();
      String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      ToolRunner.run(conf, new Division(), remainingArgs);
    } catch (Throwable t) {
      System.err.println(StringUtils.stringifyException(t));
      throw t;
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    System.out.println(Arrays.asList(args).toString());
    // Parse arguments
    Path inputPath = new Path(args[0]),
        outputPath = new Path(args[1]);
    int buckets = Integer.parseInt(args[2]);
    conf.setInt(BUCKETS_KEY, buckets);
    conf.setInt(TriplesPartitioner.ELEMENT_RANGE_KEY, buckets);

    // Setup job
    Job job = Job.getInstance(conf);
    job.setJarByClass(Division.class);

    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, inputPath);
    job.setMapperClass(DivisionMapper.class);

    job.setMapOutputKeyClass(Tuple.class);
    job.setMapOutputValueClass(Tuple.class);

    job.setPartitionerClass(TriplesPartitioner.class);

    job.setReducerClass(DivisionReducer.class);
    job.setNumReduceTasks(buckets * buckets * buckets / 6);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputPath);

    // Run job
    job.submit();
    return job.waitForCompletion(true) ? 0 : 1;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration entries) {
    this.conf = entries;
  }
}
