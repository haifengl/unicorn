/*******************************************************************************
  * (C) Copyright 2017 Haifeng Li
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *******************************************************************************/

package unicorn.unibase

import java.util.Properties
import scala.collection.JavaConverters._
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import smile.nlp.dictionary.StopWords
import smile.nlp.dictionary.{EnglishPunctuations, EnglishStopWords}

/**
  * @author Haifeng Li
  */
package object search {
  /**
    * This is a pipeline that takes in a string and returns various analyzed
    * linguistic forms. The String is tokenized via a tokenizer
    * (such as PTBTokenizerAnnotator), and then other sequence model style
    * annotation can be used to add things like lemmas, POS tags, and stop
    * word filter.
    *
    * StanfordCoreNLP loads a lot of models, we should do it only once here.
    */
  val tokenizer = {
    // Create StanfordCoreNLP object properties, with POS tagging
    // (required for lemmatization), and lemmatization.
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")

    new StanfordCoreNLP(props)
  }

  /** Tokenizes a text document including lemmatization,
    * stop word filtering. Returns the number of words
    * (after all filtering) and the frequency of each
    * unique word.
    */
  def tokenize(text: String): (Int, Map[String, Int]) = {
    val document = new Annotation(text)
    tokenizer.annotate(document)
    val tokens = document.get(classOf[TokensAnnotation])

    val stopwords = EnglishStopWords.COMPREHENSIVE
    val punctuations = EnglishPunctuations.getInstance()

    val words = tokens.asScala.map(_.get(classOf[LemmaAnnotation]).toLowerCase).filter { word =>
      println(word)
      !(stopwords.contains(word) || punctuations.contains(word))
    }

    val map = words.groupBy(identity).mapValues(_.size)
    (words.size, map)
  }
}
