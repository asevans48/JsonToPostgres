package org.aevans.goat.dbconverters

import org.aevans.goat.database.DatabaseHandler
import org.aevans.goat.configurations.CommandLineParser

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.mutable.TreeSet
import scala.collection.breakOut
import scala.collection.immutable.Queue
import scala.collection.JavaConversions._
import scala.util.matching.Regex._
import scala.concurrent.{Future,Await}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Random,Try,Success,Failure}


/**
 * This class escapes reserved terms with double quotes
 */
object EscapeReserved{
  
  def escapeWord(word : String):String={
    
     word.toLowerCase().trim match{
       case "user" | "end" | "after" |"action" | "add" | "begin" |"always" |"also" |"all"|"analyse"|"analyze"|"and"|"array"|"asc"|"asymmetric"|"binary"|"both"|"case"|"cast"|"check"|"collate"|"constraint"|"create"|"current_catalog"|"current_date"|"default"|"desc"|"distinct"|"fetch"|"forward"|"full"|"grant"|"in"|"intersect"|"leading"|"some"|"symmetric"|"then"|"to"|"using"|"window"|"with" =>{
         '"'+word+'"'
       }
       case _ => word
     }
  }
  
  def unescapeWord(word : String):String={
    word.toLowerCase().trim match{
       case "\"user\"" | "\"end\"" | "\"after\"" |"\"action\"" | "\"add\"" | "\"begin\"" |"\"always\"" |"\"also\"" |"\"all\""|"\"analyse\""|"\"analyze\""|"\"and\""|"\"array\""|"\"asc\""|"\"asymmetric\""|"\"binary\""|"\"both\""|"\"case\""|"\"cast\""|"\"check\""|"\"collate\""|"\"constraint\""|"\"create\""|"\"current_catalog\""|"\"current_date\""|"\"default\""|"\"desc\""|"\"distinct\""|"\"fetch\""|"\"forward\""|"\"full\""|"\"grant\""|"\"in\""|"\"intersect\""|"\"leading\""|"\"some\""|"\"symmetric\""|"\"then\""|"\"to\""|"\"using\""|"\"window\""|"\"with\"" =>{
         word.substring(1, word.length - 1)
       }
       case _ => word
     }
  }
  
  def escapeData(data : String):String={
    data.replaceAll("?", "??").replaceAll("'","''")
  }
}


class JsonbToPostgresConverter {
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
  
    var schema : String = null
    var jsonColumn : String = null
    var tableName : String = null
    var offset : Int = 0
    var offsetColumn  : String = null
    var maxThreads : Int = 0
    
    def getData(sql : String):Future[List[Map[String,Any]]]=Future{
      DatabaseHandler.queryForMapList(sql) 
    }
    
    def convert(recordList : List[Map[String,Any]]):Future[List[Map[String,List[Map[String,Any]]]]] = Future{
      var records: List[Map[String,List[Map[String,Any]]]] = List[Map[String,List[Map[String,Any]]]]()
      
      def buildRecords(table : String,jmap : Map[String,Any]):List[Map[String,List[Map[String,Any]]]]={
          var record : Map[String,Any] = Map[String,Any]()
          var orecords =List[Map[String,List[Map[String,Any]]]]()
      
          
          for(k <- jmap.keySet){
            val tp = jmap.get(k).getOrElse(null)
            val krep = k.replaceAll("[^A-Za-z0-9]+","").toLowerCase()
            if(!tp.isInstanceOf[Map[String,Any]] && !tp.isInstanceOf[List[String]] && !tp.isInstanceOf[List[Map[String,Any]]]){
              record = record + (EscapeReserved.escapeWord(krep) -> tp)
            }else if(tp.isInstanceOf[List[Any]]){
              for(str <- tp.asInstanceOf[List[Any]]){
                orecords = orecords ++ buildRecords(schema+"."+krep, Map[String,Any]((krep->str),("pid"->jmap.get("pid").get)))
              }
            }else if(tp.isInstanceOf[List[Map[String,Any]]]){
              for(mp <- tp.asInstanceOf[List[Map[String,Any]]]){
                orecords = orecords ++ buildRecords(schema+"."+krep,mp + ("pid"->jmap.get("pid").get))
              }
            }else if(tp.isInstanceOf[Map[String,Any]]){
              orecords = orecords ++ buildRecords(schema+"."+krep,tp.asInstanceOf[Map[String,Any]] + ("pid"->jmap.get("pid").get))    
            }
          }
          orecords = orecords ++ List(Map((table -> List[Map[String,Any]](record))))
          orecords
       }
       
      for(rec <- recordList){
        val jMap : Map[String,Any] = mapper.readValue[Map[String,Any]](rec.get(this.jsonColumn).get.asInstanceOf[String])
        records = records ++ buildRecords(this.tableName, jMap ++ (rec - jsonColumn))
      }
      
      records
    }
    
    
    def genFromSample(dataList : List[Map[String,List[Map[String,Any]]]]):Future[Boolean]=Future{      
      var rBool : Boolean = dataList.size > 0
      if(rBool){
          //must build a combination to improve SQL speed because each Json record has every table
         var tnames : Set[String] = Set[String]()
         var postList : Map[String,List[Map[String,Any]]] = Map[String,List[Map[String,Any]]]()
         
         dataList.foreach({mappings => 
            mappings.keySet.foreach({
              k =>
                if(tnames.contains(k)){
                   postList = postList.updated(k,postList.get(k).get ++ mappings.get(k).get)
                }else{
                  postList = postList + (k -> mappings.get(k).get)
                  tnames = tnames + k
                }
            })
         })
         
         //post data
         if(postList.size > 0){
           try{
            DatabaseHandler.checkSample(postList)
            rBool = true
           }catch{
            case t :Throwable =>{rBool = false}
           }
         }
      }
      rBool 
    }
    
    def post(dataList : List[Map[String,List[Map[String,Any]]]]):Future[Boolean]=Future{      
      var rBool : Boolean = dataList.size > 0
      if(rBool){
         //must build a combination to improve SQL speed because each Json record has every table
         var tnames : Set[String] = Set[String]()
         var postList : Map[String,List[Map[String,Any]]] = Map[String,List[Map[String,Any]]]()
         
         dataList.foreach({mappings => 
            mappings.keySet.foreach({
              k =>
                if(tnames.contains(k)){
                   postList = postList.updated(k,postList.get(k).get ++ mappings.get(k).get)
                }else{
                  postList = postList + (k -> mappings.get(k).get)
                  tnames = tnames + k
                }
            })
         })
         
         //post data
         if(postList.size > 0){
           try{
            DatabaseHandler.postMappingsList(postList)
            rBool = true
           }catch{
            case t :Throwable =>{
              println(t.getMessage)
              println(ExceptionUtils.getStackTrace(t))
              rBool = false
            }
           }
         }
      }
      rBool
    }
    
    def run(args : Array[String])={
      DatabaseHandler.loadDbs()
      val cmd = CommandLineParser.parse(args)
      assert(cmd.offsetNumber != null)
      assert(cmd.query != null)
      assert(cmd.outputTable != null)
      assert(cmd.offsetColumn != null)
      assert(cmd.jsonColumn != null)
      assert(cmd.schema !=  null)
      this.offsetColumn = cmd.offsetColumn
      this.schema = cmd.schema
      this.jsonColumn = cmd.jsonColumn
      this.tableName = cmd.outputTable
      this.maxThreads = cmd.maxThreads
      val sql : String = cmd.query
      
      val offInc : Int = cmd.offsetNumber
      var it : Int = 0
      val inc : Int = (offInc / this.maxThreads).toInt
      var recentRecs : Int = 1
      
      //get the sample block
      if(cmd.sampleQuery != null){
        var futs : List[Future[Boolean]] = List(this.getData(cmd.sampleQuery).flatMap({x => this.convert(x)}).flatMap { x => this.post(x) })
        val r = Await.ready(Future.sequence(futs), Duration.Inf).value.get
        r match{
          case Success(r)=>{
            println("Successfully Sampled Tables. Any Missing Tables and Columns will be inferred from the types in the Json")
          }
          case Failure(t)=>{
            println("FAILURE TO SAMPLE FOR TABLES AND COLUMNS"+t.getMessage+"\n"+ExceptionUtils.getStackFrames(t))
            System.exit(255)
          }
        }
      }
      
      while(recentRecs > 0){
          //get records and submit to parse and post
          recentRecs = 0
          var futs : List[Future[Boolean]] = List[Future[Boolean]]()
          var start = offInc * it - 1
          for(i <- 0 until maxThreads){
            futs = futs :+ this.getData(sql + " WHERE "+this.offsetColumn+" >= "+(start + i * inc)+" AND "+this.offsetColumn+" < "+(start + ((i + 1) * inc))).flatMap({x => this.convert(x)}).flatMap { x => this.post(x) }
          }
          it += 1
          
          var r = Await.result(Future.sequence(futs), Duration.Inf)
          r = r.filter{ x => x == true}
          recentRecs = r.size
      }
    }
}


object ScalaDriver{
  
  def main(args : Array[String])={
    new JsonbToPostgresConverter().run(args)
  }
  
}