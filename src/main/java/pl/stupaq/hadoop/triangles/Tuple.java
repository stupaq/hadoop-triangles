package pl.stupaq.hadoop.triangles;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Tuple extends ArrayList<Integer> implements Writable, WritableComparable<Tuple> {
  private static final String ELEMENTS_DELIMITER = ",";

  public Tuple() {
  }

  public Tuple(List<Integer> tuple) {
    super(tuple);
  }

  public Tuple(Integer... elements) {
    this(Arrays.<Integer>asList(elements));
  }

  @SuppressWarnings("unused")
  public Tuple project(List<Integer> indices) {
    Tuple result = new Tuple();
    for (Integer index : indices) {
      result.add(get(index));
    }
    return result;
  }

  @SuppressWarnings("unused")
  public Tuple strip(List<Integer> indices) {
    Tuple result = new Tuple(this);
    result.stripInPlace(indices);
    return result;
  }

  public void stripInPlace(List<Integer> indices) {
    boolean[] strip = new boolean[size()];
    for (Integer index : indices) {
      strip[index] = true;
    }
    int free, i;
    for (free = 0, i = 0; i < size(); i++) {
      if (!strip[i]) {
        set(free++, get(i));
      }
    }
    resize(free);
  }

  private void resize(int desired) {
    while (size() > desired) {
      removeLast();
    }
    ensureCapacity(desired);
    while (size() < desired) {
      add(null);
    }
  }

  public void removeLast() {
    remove(size() - 1);
  }

  @SuppressWarnings("unused")
  public void append(Tuple tuple) {
    addAll(tuple);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(size());
    for (int elem : this) {
      out.writeInt(elem);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    clear();
    int size = in.readInt();
    while (size-- > 0) {
      add(in.readInt());
    }
  }

  public Text toText() {
    if (isEmpty()) {
      return new Text("");
    }
    Iterator<Integer> iter = iterator();
    StringBuilder builder = new StringBuilder().append(iter.next());
    while (iter.hasNext()) {
      builder.append(ELEMENTS_DELIMITER).append(iter.next());
    }
    return new Text(builder.toString());
  }

  public void fromText(Text value) {
    clear();
    if (!value.toString().isEmpty()) {
      String[] elements = value.toString().split(ELEMENTS_DELIMITER);
      resize(elements.length);
      int index = 0;
      for (String elem : elements) {
        set(index++, Integer.parseInt(elem));
      }
    }
  }

  @Override
  public String toString() {
    return toText().toString();
  }

  @Override
  public int compareTo(Tuple other) {
    Iterator<? extends Comparable> iter1 = iterator(), iter2 = other.iterator();
    while (iter1.hasNext() && iter2.hasNext()) {
      @SuppressWarnings("unchecked") int ret = iter1.next().compareTo(iter2.next());
      if (ret != 0) {
        return ret;
      }
    }
    return other.size() - size();
  }
}
