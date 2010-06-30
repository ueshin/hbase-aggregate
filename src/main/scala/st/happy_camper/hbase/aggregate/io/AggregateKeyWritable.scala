package st.happy_camper.hbase.aggregate.io

import _root_.st.happy_camper.hbase.aggregate.dto.AggregateKey

import _root_.java.io.{ DataInput, DataOutput }
import _root_.java.util.Date

import _root_.org.apache.hadoop.io.{ Text, WritableComparable, WritableUtils }

class AggregateKeyWritable(var aggregateKey: Option[AggregateKey]) extends WritableComparable[AggregateKeyWritable] {

  def this() = this(None)

  def this(aggregateKey: AggregateKey) = this(Option(aggregateKey))

  def write(out: DataOutput) {
    aggregateKey map {
      aggregateKey => {
        Text.writeString(out, aggregateKey.remoteAddr)
        Text.writeString(out, aggregateKey.requestPath)
        WritableUtils.writeVLong(out, aggregateKey.requestedDate.getTime)
      }
    }
  }

  def readFields(in: DataInput) {
    aggregateKey = Some(new AggregateKey(
      Text.readString(in),
      Text.readString(in),
      new Date(WritableUtils.readVLong(in))
    ))
  }

  def compareTo(other: AggregateKeyWritable) =
    aggregateKey map {
      aggregateKey =>
        other.aggregateKey map {
          other => aggregateKey.compareTo(other)
        } getOrElse 1
    } getOrElse -1
}
