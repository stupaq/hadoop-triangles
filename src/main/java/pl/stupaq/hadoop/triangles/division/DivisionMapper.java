package pl.stupaq.hadoop.triangles.division;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pl.stupaq.hadoop.triangles.Tuple;

import java.io.IOException;
import java.util.HashSet;

class DivisionMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
  protected int buckets;

  @Override
  protected final void setup(Context context) throws IllegalStateException {
    Configuration conf = context.getConfiguration();
    buckets = conf.getInt(Division.BUCKETS_KEY, -1);
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
    for (int hi = 0; hi < buckets; hi++) {
      for (int hj = hi; hj < buckets; hj++) {
        for (int hk = hj; hk < buckets; hk++) {
          Tuple key = new Tuple(hi, hj, hk);
          HashSet<Integer> set = new HashSet<>(key);
          if (set.contains(ha) && set.contains(hb)) {
            context.write(key, new Tuple(edge));
          }
        }
      }
    }
  }
}
