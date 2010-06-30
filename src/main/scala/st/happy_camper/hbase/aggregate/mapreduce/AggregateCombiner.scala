package st.happy_camper.hbase.aggregate.mapreduce

import _root_.st.happy_camper.hbase.aggregate.io.AggregateKeyWritable

import _root_.scala.collection.JavaConversions._

import _root_.org.apache.hadoop.io.LongWritable
import _root_.org.apache.hadoop.mapreduce.Reducer

class AggregateCombiner extends Reducer[AggregateKeyWritable, LongWritable, AggregateKeyWritable, LongWritable] {

  type Context = Reducer[AggregateKeyWritable, LongWritable, AggregateKeyWritable, LongWritable]#Context

  override def reduce(key: AggregateKeyWritable, values: java.lang.Iterable[LongWritable], context: Context) {
    context.write(key, new LongWritable(values.foldLeft(0L) { _ + _.get }))
  }
}
