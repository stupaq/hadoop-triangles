package pl.stupaq.hadoop.triangles.join3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pl.stupaq.hadoop.triangles.Tuple;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

class Join3Reducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
  protected int buckets;

  @Override
  protected final void setup(Context context) throws IllegalStateException {
    Configuration conf = context.getConfiguration();
    buckets = conf.getInt(Join3.BUCKETS_KEY, -1);
    assert buckets > 0 : "Bad buckets count";
  }

  @Override
  protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
      throws IOException, InterruptedException {
    Set<Tuple> edges = new HashSet<>();
    for (Tuple e : values) {
      edges.add(new Tuple(e));
    }
    // We know that for each input edge (a, b) a < b
    int ha = key._0(), hb = key._1(), hc = key._2();
    for (Tuple e1 : edges) {
      if (ha != e1._0() % buckets) {
        continue;
      }
      for (Tuple e2 : edges) {
        if (hb != e2._0() % buckets || e1._1() != e2._0()) {
          continue;
        }
        for (Tuple e3 : edges) {
          if (hc != e3._1() % buckets || e2._1() != e3._1() || e1._0() != e3._0()) {
            continue;
          }
          Tuple triangle = new Tuple(e1._0(), e2._0(), e3._1());
          System.out.println(key + " -> " + triangle);
          context.write(NullWritable.get(), triangle.toText());
        }
      }
    }
  }
}
