package com.zetdata.hero.trial

import java.util.Collections
import java.util.Comparator
import java.util.LinkedList

import scala.collection.JavaConverters._

import org.wltea.analyzer.core.IKSegmenter

object Utils {
  
    def wordSegment(p: String): (Int, Seq[String]) = {
      var id_descriptions = p.split(",")
      val id = id_descriptions(0)
      var tokens = new LinkedList[String]()
      var ikSegmenter = new IKSegmenter(new java.io.StringReader(p), true)
      var lexeme = ikSegmenter.next();
      while (lexeme != null) {
        tokens.add(lexeme.getLexemeText());
        lexeme = ikSegmenter.next();
      }
      (id.toInt, tokens.asScala)
    }

    def nonNegativeMod(x: Int, mod: Int): Int = {
      val rawMod = x % mod
      rawMod + (if (rawMod < 0) mod else 0)
    }

    def iterFunc(x: Seq[String]):List[(Int, String)] = {
      var list = new LinkedList[(Int, String)]
      for (i <- x) { list.add((nonNegativeMod(i.##, 1 << 20), i)) }
      Collections.sort(list, new Comparator[(Int, String)]() {
        def compare(x: (Int, String), y: (Int, String)) = {
          (x._1 - y._1).toInt
        }
      })
      
      
      var unique_list = List[(Int, String)]()
      unique_list.+:(list.getFirst())
      for (j <- 1 to list.size()-1 if list.get(j) != list.get(j-1)){unique_list = unique_list.+:(list.get(j))  }
      unique_list
    }
    
}
