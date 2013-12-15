package pl.stupaq.hadoop.triangles.join3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pl.stupaq.hadoop.triangles.Tuple;

import java.io.IOException;

class Join3Mapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
  protected int buckets;

  @Override
  protected final void setup(Context context) throws IllegalStateException {
    Configuration conf = context.getConfiguration();
    buckets = conf.getInt(Join3.BUCKETS_KEY, -1);
    assert buckets > 0 : "Bad buckets count";
  }

  @Override
  protected final void map(LongWritable inputKey, Text value, Context context)
      throws IOException, InterruptedException {
    // Parse an edge
    Tuple edge = new Tuple();
    edge.fromText(value);
    // We assume that each edge is specified in input file by a pair of endpoints (a, b) where a < b
    assert edge._0() < edge._1() : "Invalid input format";
    int ha = edge._0() % buckets, hb = edge._1() % buckets;
    // We want to compute E(x, y) x E(y, z) x E(x, z), the edge we have just read can be one of
    // the following in above join:
    // E(x, y)
    for (int hi = 0; hi < buckets; hi++) {
      if (hi != hb) {
        context.write(new Tuple(ha, hb, hi), new Tuple(edge));
      }
    }
    // E(y, z)
    for (int hi = 0; hi < buckets; hi++) {
      if (hi != ha) {
        context.write(new Tuple(hi, ha, hb), new Tuple(edge));
      }
    }
    // E(x, z)
    for (int hi = 0; hi < buckets; hi++) {
      context.write(new Tuple(ha, hi, hb), new Tuple(edge));
    }
  }
}
