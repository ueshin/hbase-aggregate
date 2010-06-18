package st.happy_camper.hbase.aggregate

import _root_.st.happy_camper.hbase.aggregate.mapreduce.{ AggregateMapper, AggregateKeyWritable }

import _root_.org.apache.hadoop.hbase.HBaseConfiguration
import _root_.org.apache.hadoop.hbase.client.Scan
import _root_.org.apache.hadoop.hbase.io.ImmutableBytesWritable
import _root_.org.apache.hadoop.hbase.mapreduce.{ IdentityTableReducer, TableMapReduceUtil }

import _root_.org.apache.hadoop.fs.Path
import _root_.org.apache.hadoop.io.LongWritable
import _root_.org.apache.hadoop.mapreduce.Job
import _root_.org.apache.hadoop.mapreduce.lib.input.{ FileInputFormat, TextInputFormat }
import _root_.org.apache.hadoop.util.GenericOptionsParser

object Aggregator {

  def main(args : Array[String]) {
    val conf = new HBaseConfiguration
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs

    val job = new Job(conf, "HBase Aggregator")
    job.setJarByClass(getClass)

    val scan = new Scan
    TableMapReduceUtil.initTableMapperJob("access", scan, classOf[AggregateMapper], classOf[AggregateKeyWritable], classOf[LongWritable], job)
  }
}
