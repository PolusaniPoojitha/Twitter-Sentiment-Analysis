import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark._

//import java.io._


object TwitterSentiment  {
   def main(args : Array[String]):Unit= {
    System.setProperty("SPARK_YARN_MODE", "true") 
    // (1) config work to create a twitter object
   val conf = new SparkConf().setAppName("TwitterRealTimeAnalysis").setMaster("yarn");
    //conf.setMaster("spark://hadoopmaster-ravi-pujitha:8088");
    val sc=new SparkContext(conf);
    import org.apache.spark.streaming.{Seconds,StreamingContext}
    import org.apache.spark.streaming.twitter._
    import org.apache.spark.streaming.Duration
    val jssc = new StreamingContext(sc, Seconds(1));
    System.setProperty("twitter4j.oauth.consumerKey", "9q7jHrIX4KMDuQjFVLC1vEtno");
    System.setProperty("twitter4j.oauth.consumerSecret", "jw4GfQLmT8OB3nD7KI8aDZxu0VLLpHKvtIYFeZizMN3ORFwEwY");
    System.setProperty("twitter4j.oauth.accessToken", "2955472975-drC7s6mcLWtICaYhCBU0gXa5bVKkts6iRxQEDTz");
    System.setProperty("twitter4j.oauth.accessTokenSecret", "AyTvNWnl9yQIu3zI3RyCU2G1u6NIB1daQKpVr7IUw9VxL");
   var status:StringBuilder=new StringBuilder();
        val twitterStream=TwitterUtils.createStream(jssc, None, Array("isis"))
          twitterStream.filter(_.getLang()=="en").foreachRDD{(rdd, time) =>
             rdd.map(t => {
               (
                "userName:"->t.getUser.getScreenName,
               "location:"->t.getUser.getLocation,
                "sentiment:"-> detectSentiment(t.getText),
                "status:"-> t.getText,                 
                "FavoriteCount:"-> t.getFavoriteCount,
                "RetweetCount:"-> t.getRetweetCount
               )
               //hdfs://hadoopmaster-ravi-pujitha:9000/hadoop-user/Tweet_ISIS
             }).saveAsTextFile("hdfs://hadoopmaster-ravi-pujitha:9000/hadoop-user/Tweet_ISIS")
         }
         jssc.start();
         jssc.awaitTermination();
  }
  
  def detectSentiment(message: String): SENTIMENT_TYPE = {
    import edu.stanford.nlp.dcoref.CorefChain;
import edu.stanford.nlp.dcoref.CorefCoreAnnotations.CorefChainAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import edu.stanford.nlp.sentiment
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import java.util.Properties;
   val nlpProps = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos,lemma, parse, sentiment,ner")
    props
  }
    val pipeline = new StanfordCoreNLP(nlpProps)

    val annotation = pipeline.process(message)
    println(annotation);
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()

    var longest = 0
    var mainSentiment = 0

    for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val tree = sentence.get(classOf[edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree])
     println("The tree is"+tree)
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      println("The sentiment is"+sentiment);
      val partText = sentence.toString
      println(partText)
      if (partText.length() > longest) {
        mainSentiment = sentiment
        longest = partText.length()
      }

      sentiments += sentiment.toDouble
      sizes += partText.length

      println("debug: " + sentiment)
      println("size: " + partText.length)
    }

    val averageSentiment:Double = {
      if(sentiments.size > 0) sentiments.sum / sentiments.size
      else -1
    }

    val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weightedSentiment = weightedSentiments.sum / (sizes.fold(0)(_ + _))

    if(sentiments.size == 0) {
      mainSentiment = -1
      weightedSentiment = -1
    }
    weightedSentiment match {
      case s if s <= 0.0 => NOT_UNDERSTOOD
      case s if s < 1.0 => VERY_NEGATIVE
      case s if s < 2.0 => NEGATIVE
      case s if s < 3.0 => NEUTRAL
      case s if s < 4.0 => POSITIVE
      case s if s < 5.0 => VERY_POSITIVE
      case s if s > 5.0 => NOT_UNDERSTOOD
    }

  }

  trait SENTIMENT_TYPE
  case object VERY_NEGATIVE extends SENTIMENT_TYPE
  case object NEGATIVE extends SENTIMENT_TYPE
  case object NEUTRAL extends SENTIMENT_TYPE
  case object POSITIVE extends SENTIMENT_TYPE
  case object VERY_POSITIVE extends SENTIMENT_TYPE
  case object NOT_UNDERSTOOD extends SENTIMENT_TYPE
}