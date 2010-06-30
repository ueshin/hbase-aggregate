package st.happy_camper.hbase.aggregate

import _root_.st.happy_camper.hbase.aggregate.io.AggregateKeyWritable
import _root_.st.happy_camper.hbase.aggregate.mapreduce.{ AggregateMapper, AggregateCombiner, AggregateReducer }

import _root_.org.apache.hadoop.hbase.HBaseConfiguration
import _root_.org.apache.hadoop.hbase.client.Scan
import _root_.org.apache.hadoop.hbase.io.ImmutableBytesWritable
import _root_.org.apache.hadoop.hbase.mapreduce.{ IdentityTableReducer, TableMapReduceUtil }

import _root_.org.apache.hadoop.fs.Path
import _root_.org.apache.hadoop.io.{ LongWritable, Text }
import _root_.org.apache.hadoop.mapreduce.Job
import _root_.org.apache.hadoop.mapreduce.lib.input.{ FileInputFormat, TextInputFormat }
import _root_.org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import _root_.org.apache.hadoop.util.GenericOptionsParser

object Aggregator {

  def main(args : Array[String]) {
    val conf = new HBaseConfiguration
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs

    val job = new Job(conf, "HBase Aggregator")
    job.setJarByClass(getClass)

    val scan = new Scan
    scan.setMaxVersions(Integer.MAX_VALUE)
    TableMapReduceUtil.initTableMapperJob("access", scan, classOf[AggregateMapper], classOf[AggregateKeyWritable], classOf[LongWritable], job)

    job.setCombinerClass(classOf[AggregateCombiner])
    job.setReducerClass(classOf[AggregateReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])

    FileOutputFormat.setOutputPath(job, new Path(args(0)))

    System.exit(if(job.waitForCompletion(true)) 0 else 1)
  }
}
