package st.happy_camper.hbase.aggregate.mapreduce

import _root_.st.happy_camper.hbase.aggregate.dto.AggregateKey
import _root_.st.happy_camper.hbase.aggregate.io.{ AggregateKeyWritable, CombinedLogWritable }

import _root_.java.util.{ Calendar, Date }

import _root_.scala.collection.JavaConversions._

import _root_.org.apache.hadoop.hbase.client.Result
import _root_.org.apache.hadoop.hbase.io.ImmutableBytesWritable
import _root_.org.apache.hadoop.hbase.mapreduce.TableMapper
import _root_.org.apache.hadoop.hbase.util.{ Bytes, Writables }

import _root_.org.apache.hadoop.io.LongWritable
import _root_.org.apache.hadoop.mapreduce.Mapper

class AggregateMapper extends TableMapper[AggregateKeyWritable, LongWritable] {

  type Context = Mapper[ImmutableBytesWritable, Result, AggregateKeyWritable, LongWritable]#Context

  private val one = new LongWritable(1L)

  override def map(key: ImmutableBytesWritable, value: Result, context: Context) {
    val remoteAddr = Bytes.toString(key.get)
    val familyMap = value.getMap.get(Bytes.toBytes("log"))
    familyMap foreach {
      case (qualifier, qualifierMap) => {
        val requestPath = Bytes.toString(qualifier)
        qualifierMap foreach {
          case (version, column) => {
            val requestedTime = new Date(version.longValue)

            val combinedLogWritable = new CombinedLogWritable
            Writables.getWritable(column, combinedLogWritable)
            val combinedLog = combinedLogWritable.combinedLog

            val calendar = Calendar.getInstance
            calendar.setTime(requestedTime)
            calendar.set(Calendar.HOUR_OF_DAY, 0)
            calendar.set(Calendar.MINUTE, 0)
            calendar.set(Calendar.SECOND, 0)
            calendar.set(Calendar.MILLISECOND, 0)

            context.write(new AggregateKeyWritable(new AggregateKey(remoteAddr, if(requestPath.matches(".*/$")) { requestPath + "index.html" } else { requestPath }, calendar.getTime)), one)
          }
        }
      }
    }
  }
}
