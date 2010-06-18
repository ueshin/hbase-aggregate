package st.happy_camper.hbase.aggregate.mapreduce

import _root_.java.util.{ Calendar, Date }

import _root_.scala.collection.JavaConversions._

import _root_.org.apache.hadoop.hbase.client.Result
import _root_.org.apache.hadoop.hbase.io.ImmutableBytesWritable
import _root_.org.apache.hadoop.hbase.mapreduce.TableMapper
import _root_.org.apache.hadoop.hbase.util.Bytes

import _root_.org.apache.hadoop.io.LongWritable
import _root_.org.apache.hadoop.mapreduce.Mapper

class AggregateMapper extends TableMapper[AggregateKeyWritable, LongWritable] {

  type Context = Mapper[ImmutableBytesWritable, Result, AggregateKeyWritable, LongWritable]#Context

  private val one = new LongWritable(1L)

  override def map(key: ImmutableBytesWritable, value: Result, context: Context) {
    val remoteAddr = Bytes.toString(key.get)
    val familyMap = value.getMap.get(Bytes.toBytes("log"))
    for((qualifier, qualifierMap) <- familyMap; (version, column) <- qualifierMap) {
      val domain = Bytes.toString(qualifier)
      val requestedTime = new Date(version.longValue)
      val requestPath = Bytes.toString(column)

      val calendar = Calendar.getInstance
      calendar.setTime(requestedTime)
      calendar.set(Calendar.HOUR_OF_DAY, 0)
      calendar.set(Calendar.MINUTE, 0)
      calendar.set(Calendar.SECOND, 0)
      calendar.set(Calendar.MILLISECOND, 0)

      context.write(new AggregateKeyWritable(new AggregateKey(remoteAddr, requestPath, calendar.getTime)), one)
    }
  }
}
