package st.happy_camper.hbase.aggregate.mapreduce

import _root_.java.util.Date

import _root_.org.apache.commons.lang.builder.CompareToBuilder

class AggregateKey(val remoteAddr: String, val requestPath: String, val requestedDate: Date) extends Comparable[AggregateKey] {

  def compareTo(other: AggregateKey) = {
    new CompareToBuilder().append(remoteAddr, other.remoteAddr).append(requestPath, other.requestPath).append(requestedDate, other.requestedDate).toComparison
  }
}
