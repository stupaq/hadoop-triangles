package pl.stupaq.hadoop.triangles.join3;

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

/**
 * Cyclic three-way join, let B be cardinality of the image of our hash function (the one that we
 * use to hash vertices). As seen in {@link Join3Mapper}, each edge is replicated to 3*B-2 different
 * reducers, therefore communication cost of the algorithm is |E|*(3*B-2). This algorithm is worse
 * than {@link Join3} when comparing expected costs.
 */
public class Join3 implements Tool {
  static final String BUCKETS_KEY = "triangles.join3.buckets";
  private Configuration conf;

  public static void main(String[] args) throws Exception {
    try {
      Configuration conf = new Configuration();
      String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      ToolRunner.run(conf, new Join3(), remainingArgs);
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
    job.setJarByClass(Join3.class);

    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, inputPath);
    job.setMapperClass(Join3Mapper.class);

    job.setMapOutputKeyClass(Tuple.class);
    job.setMapOutputValueClass(Tuple.class);

    job.setPartitionerClass(TriplesPartitioner.class);

    job.setReducerClass(Join3Reducer.class);
    job.setNumReduceTasks(buckets * buckets * buckets);

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
