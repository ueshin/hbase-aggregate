package st.happy_camper.hbase.aggregate.mapreduce

import _root_.java.text.SimpleDateFormat

import _root_.scala.collection.JavaConversions._

import _root_.org.apache.hadoop.io.{ LongWritable, Text }
import _root_.org.apache.hadoop.mapreduce.Reducer

class AggregateReducer extends Reducer[AggregateKeyWritable, LongWritable, Text, LongWritable] {

  type Context = Reducer[AggregateKeyWritable, LongWritable, Text, LongWritable]#Context

  override def reduce(key: AggregateKeyWritable, values: java.lang.Iterable[LongWritable], context: Context) {
      context.write(
        new Text("%s\t%s\t%s".format(key.aggregateKey.get.remoteAddr, key.aggregateKey.get.requestPath, dateFormat.format(key.aggregateKey.get.requestedDate))),
        new LongWritable(values.foldLeft(0L) { _ + _.get })
      )
  }

  private def dateFormat = new SimpleDateFormat("yyyy/MM/dd")
}
