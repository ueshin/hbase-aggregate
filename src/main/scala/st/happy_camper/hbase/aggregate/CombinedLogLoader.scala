package st.happy_camper.hbase.aggregate

import _root_.st.happy_camper.hbase.aggregate.mapreduce.CombinedLogLoadMapper

import _root_.org.apache.hadoop.hbase.HBaseConfiguration
import _root_.org.apache.hadoop.hbase.client.Put
import _root_.org.apache.hadoop.hbase.io.ImmutableBytesWritable
import _root_.org.apache.hadoop.hbase.mapreduce.{ IdentityTableReducer, TableMapReduceUtil }

import _root_.org.apache.hadoop.fs.Path
import _root_.org.apache.hadoop.mapreduce.Job
import _root_.org.apache.hadoop.mapreduce.lib.input.{ FileInputFormat, TextInputFormat }
import _root_.org.apache.hadoop.util.GenericOptionsParser

object CombinedLogLoader {

  def main(args: Array[String]) {
    val conf = new HBaseConfiguration
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs

    val job = new Job(conf, "CombinedLog Loader")
    job.setJarByClass(getClass)

    job.setMapperClass(classOf[CombinedLogLoadMapper])
    job.setInputFormatClass(classOf[TextInputFormat])
    FileInputFormat.setInputPaths(job, new Path(otherArgs(0)))

    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[Put])

    TableMapReduceUtil.initTableReducerJob("access", classOf[IdentityTableReducer], job)

    System.exit(if(job.waitForCompletion(true)) 0 else 1)
  }
}
