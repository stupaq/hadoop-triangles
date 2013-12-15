package pl.stupaq.hadoop.triangles.join3;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

import pl.stupaq.hadoop.triangles.Tuple;

class Join3Partitioner extends Partitioner<Tuple, Tuple> implements Configurable {
  private Configuration conf;
  private int buckets;

  @Override
  public int getPartition(Tuple key, Tuple value, int reducersCount) {
    // This ensures that in case reducersCount == buckets^3 then we get perfect
    // distribution of keys.
    return ((key._0() * buckets + key._1()) * buckets + key._2()) % reducersCount;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    buckets = conf.getInt(Join3.BUCKETS_KEY, -1);
    assert buckets > 0 : "Bad buckets count";
  }
}
