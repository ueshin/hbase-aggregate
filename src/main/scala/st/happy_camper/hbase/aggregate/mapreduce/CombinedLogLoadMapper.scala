package st.happy_camper.hbase.aggregate.mapreduce

import _root_.st.happy_camper.hbase.aggregate.CombinedLog
import _root_.st.happy_camper.hbase.aggregate.parser.CombinedLogParser

import _root_.org.apache.hadoop.hbase.client.Put
import _root_.org.apache.hadoop.hbase.io.ImmutableBytesWritable
import _root_.org.apache.hadoop.hbase.util.Bytes

import _root_.org.apache.hadoop.io.{ LongWritable, Text }
import _root_.org.apache.hadoop.mapreduce.{ JobContext, Mapper }

import _root_.scala.util.parsing.input.CharSequenceReader

class CombinedLogLoadMapper extends Mapper[LongWritable, Text, ImmutableBytesWritable, Put] {

  type Context = Mapper[LongWritable, Text, ImmutableBytesWritable, Put]#Context

  override def map(key: LongWritable, value: Text, context: Context) {
    CombinedLogParser.parse(new CharSequenceReader(value.toString)) map {
      case CombinedLog(remoteHost, remoteUser, requestedTime, method, requestPath, protocol, statusCode, contentLength, referer, userAgent) => {
        val put = new Put(Bytes.toBytes(remoteHost))
        put.add(Bytes.toBytes("log"), Bytes.toBytes(CombinedLogLoadMapper.getDomain(context)), requestedTime.getTime, Bytes.toBytes(requestPath))
        context.write(new ImmutableBytesWritable(Bytes.toBytes(remoteHost)), put)
      }
    }
  }
}

object CombinedLogLoadMapper {

  private val JOB_CONF_KEY_DOMAIN = getClass.getName + ".domain"

  def setDomain(jobContext: JobContext, domain: String) {
    jobContext.getConfiguration.set(JOB_CONF_KEY_DOMAIN, domain)
  }

  def getDomain(jobContext: JobContext) = jobContext.getConfiguration.get(JOB_CONF_KEY_DOMAIN)
}