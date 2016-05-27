package com.hygenics.configurations

import scopt.OptionParser
import scopt._

import org.apache.commons.lang3.exception.ExceptionUtils

final class BadOptionsException(msg:String) extends RuntimeException
case class Config(confFile:String = null,kwargs: Map[String,String] = Map[String,String]())

/**
 * Parse Command Line Arguments.
 * 
 * @author aevans
 */
object CommandLineParser {
  
  var parser = new scopt.OptionParser[Config]("Scala Single Node Scheduler Program"){
    head("Scala Single Node Scheduler Program")
    opt[String]('c',"conf") action { (x,c) => c.copy(confFile = x)} text("confFile is the Configuration File to use.") validate {x => 
      if(!new java.io.File(x).exists) failure("Option --conf or -c must Contain a Valid File. File does not Exist!")  else success
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