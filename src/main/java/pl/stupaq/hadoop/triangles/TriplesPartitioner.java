package pl.stupaq.hadoop.triangles;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class TriplesPartitioner extends Partitioner<Tuple, Tuple> implements Configurable {
  public static final String ELEMENT_RANGE_KEY = "partitioner.triples.element.range";
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
    buckets = conf.getInt(ELEMENT_RANGE_KEY, -1);
    assert buckets > 0 : "Bad element range";
  }
}
