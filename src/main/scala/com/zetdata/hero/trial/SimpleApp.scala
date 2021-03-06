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
import java.util.PriorityQueue
import scala.collection.JavaConverters._
import java.util.HashMap
import java.util.HashSet

object SimpleApp {

  def main(args: Array[String]) {


    val jdFile = args(0)

    val rsFile = args(1)

    val match_result_dir = args(2)
    
    val match_analysis_details = args(3) 
    
    val white_list = args(4)
    
    val conf = new SparkConf().setAppName("CosineMatch Application")
    val sc = new SparkContext(conf)
    
    val white_rdd = sc.textFile(white_list)
    
    def make_white_set(raw:RDD[String]):Set[String]={
      var javaSet = new HashSet[String]
      for(item<-raw.toLocalIterator if item.trim() !=""){
        javaSet.add(item.toLowerCase())
      }
      javaSet.asScala.toSet[String]
    }
    
    
    val white_dict = make_white_set(white_rdd)

    def usefulWordsFiltering(tuple:(Int, Seq[String]),dict:Set[String]):(Int, Seq[String]) = {
      var usefulwords = new LinkedList[String]
      for(word <- tuple._2){
        if(dict.contains(word.toLowerCase())){ usefulwords.add(word.toLowerCase())}
      }
      (tuple._1,usefulwords.asScala.toSeq)
    }
    
    val rs_documents: RDD[(Int, Seq[String])] = sc.textFile(rsFile).map(Utils.wordSegment).map(usefulWordsFiltering(_,white_dict)).filter(_._2.length!=0)

    val jd_documents: RDD[(Int, Seq[String])] = sc.textFile(jdFile).map(Utils.wordSegment).map(usefulWordsFiltering(_,white_dict)).filter(_._2.length!=0)

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

    def cosine_match(tuple: ((Int, Array[Int], Array[Double]), (Int, Array[Int], Array[Double]))): ((Int, Int, Double), (Int, Int, List[(Int, Double)])) = {
      var score: Double = 0.0
      var match_item = List[(Int, Double)]()

      var xi = 0
      var yi = 0
      while (xi < tuple._1._2.length && yi < tuple._2._2.length) {

        if (tuple._1._2(xi) == tuple._2._2(yi)) {
          match_item = match_item.:+((tuple._1._2(xi), Math.max(tuple._1._3(xi), tuple._2._3(yi))))
          score = score + Math.max(tuple._1._3(xi), tuple._2._3(yi))
          xi = xi + 1
          yi = yi + 1
        } else if (tuple._1._2(xi) < tuple._2._2(yi)) {
          xi = xi + 1
        } else {
          yi = yi + 1
        }

      }

      ((tuple._1._1, tuple._2._1, score / Math.sqrt(tuple._1._2.length * tuple._2._2.length)), (tuple._1._1, tuple._2._1, match_item))
    }

    def heapSelect(iter: Iterable[(Int, Int, Double)], K: Int) = {
      val myComparator = new Comparator[(Int, Int, Double)]() {
        def compare(x: ((Int, Int, Double)), y: ((Int, Int, Double))) = {
          (x._3 - y._3).toInt
        }
      }
      var heap = new PriorityQueue(10, myComparator)
      for (i <- iter) {
        if (heap.size() < K || i._3 > heap.peek()._3) {
          if (heap.size() == K) heap.remove(heap.peek())
          heap.offer(i)
        }
      }
      var topList = new LinkedList[(Int, Int, Double)]()
      while (!heap.isEmpty()) {
        topList.add(heap.poll())
      }
      topList.asScala
    }

    def topN(tuple: ((Int, Iterable[(Int, Int, Double)]))): List[(Int, Int, Double)] = {
      val topList = heapSelect(tuple._2, 10)
      topList.toList
    }

    def getDict(rs_hashid: RDD[(Int, List[(Int, String)])], jd_hashid: RDD[(Int, List[(Int, String)])]): Map[Int, String] = {
      //      val super_set = hash_id_a.++(hash_id_b).reduce(_)
      val dict_rdd = rs_hashid.map(_._2).++(jd_hashid.map(_._2)).flatMap(x => x).distinct().sortBy(_._1, true)

      var dict = new HashMap[Int, String]
      for (word <- dict_rdd.toLocalIterator) {
        dict.put(word._1, word._2)
      }
      dict.asScala.toMap[Int, String]
    }

    def mapping(dict: Map[Int, String])(tuple: (Int, Int, List[(Int, Double)])): (Int, Int, String) = {
      var match_list = ""
      var first = true
      for (item <- tuple._3) {
        if (!first) match_list = match_list+"####"
        match_list = match_list + "%s".format(dict.get(item._1)) + ":" + item._2 
        first = false
      }
      (tuple._1, tuple._2, match_list)

    }


    val rs_id_tfidf = cal_tfidf(rs_documents)
    val jd_id_tfidf = cal_tfidf(jd_documents)

    val rs_hashid = cal_hashid(rs_documents)
    val jd_hashid = cal_hashid(jd_documents)

    rs_id_tfidf.cache()
    jd_id_tfidf.cache()
    val match_result_analysis = rs_id_tfidf.cartesian(jd_id_tfidf).map(cosine_match)

    val match_score_raw = match_result_analysis.map(_._1)

    val match_score_candid_part = match_score_raw.filter(_._3 > 0.00001).groupBy(_._1)
    match_score_candid_part.cache()
    val match_score_candid_part_topN = match_score_candid_part.map(topN)

    val match_score_candid_part_topN_flat = match_score_candid_part_topN.flatMap(_.iterator).map((tuple: (Int, Int, Double)) => (tuple._1, tuple._2, tuple._3, 1))

    val match_score_huntor_part = match_score_raw.filter(_._3 > 0.00001).map((tuple: (Int, Int, Double)) => (tuple._2, tuple._1, tuple._3)).groupBy(_._1)
    match_score_huntor_part.cache()
    val match_score_huntor_part_topN = match_score_huntor_part.map(topN)
    val match_score_huntor_part_topN_flat = match_score_huntor_part_topN.flatMap(_.iterator).map((tuple: (Int, Int, Double)) => (tuple._1, tuple._2, tuple._3, 0))

    val match_analsis = match_result_analysis.map(_._2)

    match_analsis.cache()

    val match_analysis_human = match_analsis.map(mapping(getDict(rs_hashid, jd_hashid))).filter(_._3 != "")
    match_score_candid_part_topN_flat.++(match_score_huntor_part_topN_flat).coalesce(1, false).map(tuple => "%s,%s,%s,%s".format(tuple._1, tuple._2, tuple._3, tuple._4)).saveAsTextFile(match_result_dir)
    match_analysis_human.coalesce(1, false).map(tuple => "%s,%s,'%s'".format(tuple._1, tuple._2, tuple._3.toString)).saveAsTextFile(match_analysis_details)
    
  }
}


