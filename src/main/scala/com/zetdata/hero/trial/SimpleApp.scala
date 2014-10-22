package com.zetdata.hero.trial
/* SimpleApp.scala */

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.IDF
import org.wltea.analyzer.core.IKSegmenter
import org.wltea.analyzer.core.Lexeme
import java.util.LinkedList
import java.util.Collections
import java.util.Comparator
import org.apache.spark.mllib.linalg.SparseVector


object SimpleApp {

  def main(args: Array[String]) {

    val rawFile = "/home/ansibler/jiaqi-sbt/simple/testData.txt"
//    val jdFile = "/home/ansibler/jiaqi-sbt/simple/for-tfidf/jd.txt"
    val jdFile = "hdfs://10.10.0.114/tmp/zetjob/admin/job320/blk1773/dump_dir"
      
      
//    val rsFile = "/home/ansibler/jiaqi-sbt/simple/for-tfidf/rs.txt"
    val rsFile = "hdfs://10.10.0.114/tmp/zetjob/admin/job320/blk1774/dump_dir"
    val conf = new SparkConf().setAppName("CosineMatch Application")
    val sc = new SparkContext(conf)

    val rs_documents: RDD[(Int, Seq[String])] = sc.textFile(rsFile).map(Utils.wordSegment)
    
    val jd_documents:RDD[(Int, Seq[String])] = sc.textFile(jdFile).map(Utils.wordSegment)
    

    def cal_tfidf(documents: RDD[(Int, Seq[String])]): RDD[(Int, Array[Int], Array[Double])] = {
      val ids: RDD[Int] = documents.map((tuple: (Int, Seq[String])) => tuple._1)
      val descriptions: RDD[Seq[String]] = documents.map((tuple: (Int, Seq[String])) => tuple._2)
      val hashingTF = new HashingTF(1 << 20)
      val tf: RDD[Vector] = hashingTF.transform(descriptions)
      tf.cache()
      val idf = new IDF().fit(tf)
      val tfidf: RDD[Vector] = idf.transform(tf)
      val id_tfidf = ids.zip(tfidf)
      id_tfidf.map((tuple: (Int, Vector)) => (tuple._1, tuple._2.asInstanceOf[SparseVector].indices, tuple._2.asInstanceOf[SparseVector].values))

    }

    def cal_hashid(documents: RDD[(Int, Seq[String])]): RDD[(Int, List[(Int, String)])] = {
      val ids: RDD[Int] = documents.map((tuple: (Int, Seq[String])) => tuple._1)
      val descriptions: RDD[Seq[String]] = documents.map((tuple: (Int, Seq[String])) => tuple._2)
      ids.zip(descriptions.map(Utils.iterFunc))
    }

    def cosine_match(tuple: ((Int, Array[Int], Array[Double]), (Int, Array[Int], Array[Double]))): ((Int, Int, Double),(Int,Int,List[(Int,Double)])) = {
      var score: Double = 0.0
      var match_item = List[(Int,Double)]()
      var xi = 0
      var yi = 0
      while (xi < tuple._1._2.length && yi < tuple._2._2.length) {

        if (tuple._1._2(xi) == tuple._2._2(yi)) {
          match_item = match_item.+:((tuple._1._2(xi), Math.max(tuple._1._3(xi), tuple._2._3(yi))))
          score = score + 1.0 //Math.max(tuple._1._3(xi), tuple._2._3(yi))
          xi = xi + 1
          yi = yi + 1
        } else if (tuple._1._2(xi) < tuple._2._2(yi)) {
          xi = xi + 1
        } else {
          yi = yi + 1
        }

      }
      ((tuple._1._1,tuple._2._1,score/Math.sqrt(tuple._1._2.length*tuple._2._2.length)),(tuple._1._1,tuple._2._1,match_item))
    }

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD
    case class MatchResult(id1: Int, id2: Int,score:Double)
    
    
    val rs_id_tfidf = cal_tfidf(rs_documents)
    val jd_id_tfidf = cal_tfidf(jd_documents)

    val rs_hashid = cal_hashid(rs_documents)
    val jd_hashid = cal_hashid(jd_documents)    
    rs_id_tfidf.cache()
    jd_id_tfidf.cache()
    val match_result_analysis = rs_id_tfidf.cartesian(jd_id_tfidf).map(cosine_match)
    
    val match_score = match_result_analysis.map(_._1)
    val match_analsis = match_result_analysis.map(_._2)

    match_score.coalesce(1, false).map(tuple => "%s,%s,%s".format(tuple._1,tuple._2,tuple._3 )).saveAsTextFile("hdfs://10.10.0.114/tmp/jiaqi-sbt/match_scoreSchema")
    
/*    val match_score_schema = match_score.map(tuple => MatchResult(tuple._1,tuple._2,tuple._3))
    match_score_schema.saveAsTextFile("./match_scoreSchema")*/
    
//    match_score.map((_._1,_._2,_._3))
    
/*    match_score.coalesce(1, false).saveAsTextFile("./match_score")
    match_analsis.coalesce(1, false).saveAsTextFile("./match_analsis")*/
  }
}


