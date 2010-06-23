package st.happy_camper.hbase.aggregate.mapreduce

import _root_.st.happy_camper.hbase.aggregate.{ CombinedLog, CombinedLogWritable }
import _root_.st.happy_camper.hbase.aggregate.parser.CombinedLogParser

import _root_.org.apache.hadoop.hbase.client.Put
import _root_.org.apache.hadoop.hbase.io.ImmutableBytesWritable
import _root_.org.apache.hadoop.hbase.util.{ Bytes, Writables }

import _root_.org.apache.hadoop.io.{ LongWritable, Text }
import _root_.org.apache.hadoop.mapreduce.Mapper

import _root_.scala.util.parsing.input.CharSequenceReader

class CombinedLogLoadMapper extends Mapper[LongWritable, Text, ImmutableBytesWritable, Put] {

  type Context = Mapper[LongWritable, Text, ImmutableBytesWritable, Put]#Context

  override def map(key: LongWritable, value: Text, context: Context) {
    CombinedLogParser.parse(new CharSequenceReader(value.toString)) map {
      case combinedLog =>
        val put = new Put(Bytes.toBytes(combinedLog.remoteHost))
        put.add(Bytes.toBytes("log"), Bytes.toBytes(combinedLog.requestPath), combinedLog.requestedTime.getTime, Writables.getBytes(new CombinedLogWritable(combinedLog)))
        context.write(new ImmutableBytesWritable(Bytes.toBytes(combinedLog.remoteHost)), put)
    }
  }
}
