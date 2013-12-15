package pl.stupaq.hadoop.triangles.join3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pl.stupaq.hadoop.triangles.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Join3Reducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
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
    List<Tuple> edges = new ArrayList<>();
    for (Tuple e : values) {
      edges.add(new Tuple(e));
    }
    // We know that for each input edge (a, b) a < b
    for (Tuple e1 : edges) {
      for (Tuple e2 : edges) {
        if (!e1.get(1).equals(e2.get(0))) {
          continue;
        }
        for (Tuple e3 : edges) {
          if (!e2.get(1).equals(e3.get(1)) || !e1.get(0).equals(e3.get(0))) {
            continue;
          }
          context.write(NullWritable.get(), new Tuple(e1.get(0), e2.get(0), e3.get(1)).toText());
        }
      }
    }
  }
}
