package com.hygenics.configurations

import scopt.OptionParser
import scopt._

import org.apache.commons.lang3.exception.ExceptionUtils

final class BadOptionsException(msg:String) extends RuntimeException
case class Config(offsetColumn:String = null,offsetNumber : Int = 100,query : String = null, outputTable : String = null, jsonColumn : String = null, schema : String = null, kwargs: Map[String,String] = Map[String,String](),maxThreads : Int = Runtime.getRuntime.availableProcessors(),sampleQuery : String = null)

/**
 * Parse Command Line Arguments.
 * 
 * @author Andrew Evans
 * Copyright 2016
 * License : Free BSD
 */
object CommandLineParser {
  
  var parser = new scopt.OptionParser[Config]("Scala Single Node Scheduler Program"){
    head("JsonB to SQL PostgreSQL Converter")
    opt[String]('c',"offsetColumn") action { (x,c) => c.copy(offsetColumn = x)} text("The offset column.") validate {x => 
      if(x == null) failure("Option -c or --offsetColumn must be specified!")  else success
    }
    opt[Int]('n',"offsetNumber") action { (x,c) => c.copy(offsetNumber = x)} text("Number to offset the batch with.") validate {x => 
      if(x <= 0) failure("offsetNumber Must be > 0")  else success
    }
    opt[String]('q',"query") action { (x,c) => c.copy(query = x)} text("Query for obtaining the table.") validate {x => 
      if(x == null) failure("Must Specify Input Table (-q or --query)")  else success
    }
    opt[String]('o',"outputTable") action { (x,c) => c.copy(outputTable = x)} text("Output Table is the output Table to use.") validate {x => 
      if(x == null) failure("Must Specify Output Table (-o or --outputTable)")  else success
    }
    opt[String]('j',"jsonColumn") action { (x,c) => c.copy(jsonColumn = x)} text("Column where the Json is contained.") validate {x => 
      if(x == null) failure("Must Specify Output Table (-j or --jsonColumn)")  else success
    }
    opt[String]('s',"schema") action { (x,c) => c.copy(schema = x)} text("Schema name for the tables.") validate {x => 
      if(x == null) failure("Must Specify Output Table (-s or --schema)")  else success
    }
    opt[String]('b',"sampleQuery") action { (x,c) => c.copy(sampleQuery = x)} text("Sample Query for building tables.") validate {x => 
      if(x == null) failure("Must Specify Output Table (-b or --sampleQuery)")  else success
    }
    opt[Int]('t',"maxThreads") action { (x,c) => c.copy(maxThreads = x)} text("Maximum number of threads.") validate {x => 
      if(x < 1) failure("Max Threads must be greater than 0.")  else success
    }
    opt[Map[String,String]]("kwargs") valueName("k1=v1,k2=v2...") action{ (x,c) => c.copy(kwargs = x) } text("Additional Arguments to Use. Pleas see the Documentation.") 
   }
  
  
  /**
   * Parses Command Line Arguments 
   */
  def parse(args:Array[String]):Config={
     parser.parse(args,Config()) match {
       case Some(config) => return config
       case None =>{
         try{
           throw new BadOptionsException("Failed to Parse Arguments")
         }catch{
           case e:BadOptionsException => println(e.getMessage+"\n"+ExceptionUtils.getStackTrace(e))
           case t:Throwable => println("UnKnown Error: "+t.getMessage+"\n"+ExceptionUtils.getStackTrace(t))
         }
       }
     }
     null
  }
  
  
}