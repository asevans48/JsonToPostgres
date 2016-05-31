package com.hygenics.database

import scalikejdbc._
import scalikejdbc.config._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


/**
 * A building list of functions for Inserting and Retreiving data
 * from a database. Could be used in place of the JDBC Template in java
 * and can do things like batch update.
 * 
 * As of now, PostgreSQL is full supported and Oracle somewhat. DSL doesn't really
 * work without first knowing a table name, sorry. I'll get around to writing the
 * queries using the environment variable || driverClassName check from 
 * https://github.com/seratch/scalikejdbc/blob/master/scalikejdbc-interpolation/src/test/scala/scalikejdbc/QueryInterfaceSpec.scala
 * 
 * @author aevans
 */
object DatabaseHandler {
 private val mapper=new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  private var dbName:Symbol = 'legacy
  private var mappingList:scala.collection.immutable.Map[String,List[scala.collection.immutable.Map[String,Any]]] = scala.collection.immutable.Map[String,List[scala.collection.immutable.Map[String,Any]]]()
  private var mapSize:Int =0
  
  
  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(
    enabled = false,
    singleLineMode = true,
    printUnprocessedStackTrace = false,
    stackTraceDepth= 15,
    logLevel = 'debug,
    warningEnabled = false,
    warningThresholdMillis = 3000L,
    warningLogLevel = 'warn
  )
  
  
  
  /**
   * Load Databases from typesafe Config 
   * @see <a href="http://scalikejdbc.org/documentation/configuration.html"/>
   * @see <a href="https://github.com/typesafehub/config"/>
   */
  def loadDbs()={
    DBs.setupAll()
  }
  
  /**
   * This should not be necessary with autocommit mode.
   */
  def commit()={
    NamedDB(dbName).commit
  }
  
  /**
   * Set the Database Name to use with the typesafe Configurator
   * @param    dbName    The database name to use from the typesafe config config (e.g. db.[DATABASE NAME/dbName].option
   */
  def setDbName(dbName:Symbol){
    this.dbName = dbName
  }
  
  /**
   * Check for the existance of a column.
   * @param    table    The table in the form table or schema.table
   * @param    column   The column
   */
  def columnExists(table:String,column:String):Boolean={ 
    var res:Boolean=false
    val tarr = table.split("\\.")
    val query = if(tarr.length == 2)"SELECT * FROM information_schema.columns WHERE table_name ILIKE '"+tarr(1)+"' AND table_schema ILIKE '"+tarr(0)+"' AND column_name ILIKE '"+column+"'" else "SELECT * FROM information_schema.columns WHERE table_name ILIKE '"+table+"'  AND column_name ILIKE '"+column+"'"
    val table_exists = if(tarr.length == 1) false else this.tableExists(tarr(1), tarr(0))
    if(table_exists){
    NamedDB(dbName) readOnly{implicit session =>
        SQL(query).list.foreach { rs =>
            
            res = true
        }
      }
    }
    res
  }
  
  /**
   * Check for the existance of a table
   * @param  table                      The table in the form table or schema.table
   * @param  schema                     The schema to use if it exists at all. Otherwise, don't use it
   * @param  {String } [driverType]     The driver name to use (e.g. pg for postgresql or oracle for OracleSQL
   */
  def tableExists(table:String,schema:String = null):Boolean={
    
    var res:Boolean=false
    NamedDB(dbName) readOnly{implicit session =>
      if(sys.env.get("SCALIKEJDBC_DATABASE").exists { _.contains("postgres") }){
        if(schema != null){
          SQL(s"SELECT * FROM information_schema.tables WHERE table_name ILIKE '$table' AND table_schema ILIKE '$schema'").list.foreach { x => res = true}
        }else{
          SQL(s"SELECT * FROM information_schema.tables WHERE table_name ILIKE '$table'").list.foreach { x => res = true}
        }
      }else if(sys.env.get("SCALIKEJDBC_DATABASE").exists { _.contains("oracle") }){
        SQL(s"SELECT * FROM user_tables WHERE table_name WHERE upper(table_name) LIKE upper('$table')").list.foreach(x => res = true)
      }else if(sys.env.get("SCALIKEJDBC_DATABASE") == None){
        SQL(s"SELECT * FROM information_schema.tables WHERE table_name ILIKE '$table' AND table_schema ILIKE '$schema'").list.foreach { x => res = true}
      }else{
        println("Driver "+sys.env.get("SCALIKEJDBC_DATABASE").get+" NOT SUPPORTED at this time. Sorry")
      }
    }
    res
  }
  
  
  /**
   * Gets a count from a table.
   * @param    table                    The table in the format table or schema.table using query for count.
   */
  def queryForRecordCount(table:String):Int ={
    NamedDB(dbName) readOnly { implicit session =>
      SQL(s"SELECT count(*) FROM $table").map(rs => rs.int(0)).single.apply().get
     }
     
  }
  
  
  
  /**
   * Gets a column count from the database.
   */
  def getColumnCount(table:String,column:String):Int={
    NamedDB(dbName) readOnly { implicit session =>
      SQL(s"SELECT count(*) FROM $table WHERE $column IS NOT NULL AND length(trim($column)) > 0").map(rs => rs.int(0)).single.apply().get
     }
  }
  
  /**
   * Execute a SQL query
   * 
   * @param    query               the query to use
   * @see      Database#setDbName
   */
  def execute(query:String)={
    NamedDB(dbName) localTx { implicit session =>
      SQL(s"$query").execute.apply()
    } 
  }
  
  /**
   * Execute a SQL query
   * 
   * @param    query               the query to use
   * @see      Database#setDbName
   */
  def update(query:String)={
    NamedDB(dbName) localTx { implicit session =>
      SQL(s"$query").update.apply()
    } 
  }
  
  /**
   * The extractor for queryForMapList
   */
  private def mapExtractor(rs: WrappedResultSet):Map[String,Any] ={
     var map:Map[String,Any] = Map[String,Any]()
     (1 to rs.metaData.getColumnCount).foreach{ k =>     
          map = map +(rs.metaData.getColumnName(k) -> rs.any(k))      
     }
     map
  }
  
  
  /**
   * Returns a mappings list containing each row of data in a map.
   * Actually implements a double for loop
   * 
   * @param    query               the query to use
   * @see      Database#setDbName
   * @return                       List[Map[String,Any]] each row as a Map of column -> data in a list  
   */
  def queryForMapList(query:String):List[Map[String,Any]]={
    NamedDB(dbName) readOnly { implicit session =>
       SQL(s"$query").map { rs => mapExtractor(rs)}.list.apply()
    }
  }
  
  /**
   * The Extractor for Json taking in a result set
   */
  private def jsonExtractor(rs: WrappedResultSet): String = {
     var map:Map[String,Any] = Map[String,Any]()
     (1 to rs.metaData.getColumnCount).foreach{ k =>     
          map = map +(rs.metaData.getColumnClassName(k) -> rs.any(k))      
     }
    mapper.writeValueAsString(map)
  }
  
  /**
   * Returns a list containing each row of data in a json string
   * Actually implements multiple for loops
   * 
   * @param    query      the query to use
   * @return              List[String] each row as a string of json data stored in a list
   * @see      Database#setDbName
   * 
   */
  def queryForJson(query:String):List[String] ={
    
    NamedDB(dbName) readOnly { implicit session =>
       SQL(s"$query").map { rs => jsonExtractor(rs)}.list.apply()
    }
  }
  
  /**
   * Post data from a Map to the database
   * 
   * @param    data     A mapping in the form Map[String,Any] containing column -> data pairs
   */
  def postMap(data:scala.collection.immutable.Map[String,Any],table:String)={
      if(data.size > 0){
         var sql=s"INSERT INTO $table ("
         var keys=""
         var vals=" VALUES('"
         
         data.keySet.foreach { k =>
             if(keys.length() == 0){
               keys += k
               vals += data.get(k).get +"'"
             }else{
               keys +=","+k
               vals += ",'"+data.get(k).get+"'"
             }
         }
         sql+=keys+") "+vals+")"
         
         NamedDB(dbName) localTx { implicit session =>
             SQL(sql).update.apply()
         }
      }
  }
  
  /**
   * A batch update version of postMap
   * @param    data                A mapping in the form Map[String,Any] containing column -> data pairs
   * @param    table               The table to insert into
   * @see Database#postMap
   */
  def batchUpdate(data:List[scala.collection.immutable.Map[String,Any]],table:String)={
    var keys: Set[String] = Set[String]()
    data.foreach{
      x =>
        x.foreach { k =>
          keys = keys + k._1
        }
    }
    
    var output:Seq[Seq[Any]] = Seq[Seq[Any]]()
    data.foreach{ map =>
      var nseq:Seq[Any] = Seq[Any]()
       keys.foreach { k =>
           if(map.contains(k)){
             nseq = nseq ++ Seq(map.get(k).get)
           }else{
             nseq = nseq ++ Seq(null)
           }
       }
      output = output ++ Seq(nseq)
    }
    
    var sql:String =s"INSERT INTO $table ("
    var cols: String = ""
    var inserts: String =" VALUES ("
    
    for(k <- keys){
      if(cols.length() ==0){
        cols += k
        inserts += "?"
      }else{
        cols+=","+k
        inserts += ",?"
      }
    }
          
    sql += cols+") "+inserts+")"
    
    NamedDB(dbName) localTx { implicit session =>
      SQL(sql).batch(output: _*).apply()
    }
    
  }
  
  /**
   * Creates a new Table
   * 
   * @param   table          The table to create
   * @param   attributes     The attributes to use in creating the table as Set[(attribute,value_type)]
   */
  def createTable(table:String, attributes:Set[(String,String)])={
     var query = s"CREATE TABLE IF NOT EXISTS $table ("+attributes.map(av => av._1+" "+av._2).mkString(",").trim+")"
     this.update(query)
  }
  
  def checkAndCreateSchema(schema:String)={
    var query = s"CREATE SCHEMA IF NOT EXISTS $schema"
    this.update(query)
  }
  
  /**
   * Checks for and Creats a column where one does not exist.
   * 
   * @param   table       The table to create.
   * @param   column      The column to use.
   * @param   val_type    The value type to use.
   */
  def checkAndCreateColumn(table:String,column:String,val_type:String = "text")={
    if(!this.columnExists(table, column)){
      val query = s"ALTER TABLE $table ADD COLUMN $column $val_type"
      this.update(query)
    }
  }
  
  /**
   * Posts Mapping Lists to a table. The mappings are contained in a Mapping of [table_name,List[Map[column,data]]]
   * This is mainly for crawling and extraction where original data is best stored in strings pre-conversion
   * so that no data is lost due to encodings and other issues. New tables and columns are created using strings.
   * 
   * This method creates new tables and columns where none exist. A future additional method may include a mapping
   * for values as well but that defeats the purpose of not needing to know every column name.
   * 
   * @param    data                  A mapping in the form Map[String,List[Map[String,Any]]] containing column -> data pairs
   * @see      Database#batchUpdate
   */
  def postMappingsList(data:scala.collection.immutable.Map[String,List[scala.collection.immutable.Map[String,Any]]]):Unit={
     data.keySet.foreach{
       table =>
         //check for table existance
         val tarr = table.split("\\.")
         
         if(tarr.length ==2){
           this.checkAndCreateSchema(tarr(0))
         }
         
         var types : Map[String,String] = Map[String,String]()
         val exists = if(tarr.length ==2)this.tableExists(tarr(1),tarr(0)) else this.tableExists(tarr(0), null)
         var keys: Set[String] = Set[String]() 
         var mp : Map[String,Any] = Map[String,Any]() 
         data.get(table).get.foreach({
             m =>
                mp = mp ++ m
         })
         
         
         for(k <- mp.keySet.filterNot { x => keys.contains(x) }){
                val d = mp.get(k).get
                if(d.isInstanceOf[String]){
                  types = types + (k -> "text")
                }else if(d.isInstanceOf[Int] || d.isInstanceOf[Integer]){
                  types = types + (k -> "integer")
                }else if(d.isInstanceOf[Double]){
                  types = types + (k -> "DOUBLE PRECISION")
                }else if(d.isInstanceOf[Float]){
                  types = types + (k -> "real")
                }else if(d.isInstanceOf[Long]){
                  types = types + (k -> "bigint")
                }else if(d.isInstanceOf[Char]){
                  types = types +(k -> "varchar(1)")
                }else if(d.isInstanceOf[Boolean]){
                  types = types + (k -> "bit")
                }else if(d.isInstanceOf[Array[Byte]]){
                  types = types + (k -> "VARBINARY")
                }else if(d.isInstanceOf[java.sql.Date]){
                  types = types + (k -> "DATE")
                }else if(d.isInstanceOf[java.sql.Timestamp]){
                  types = types + (k -> "TIMESTAMP")
                }else if(d.isInstanceOf[java.sql.Time]){
                  types = types + (k -> "TIME")
                }else if(d.isInstanceOf[java.math.BigDecimal]){
                  types = types + (k -> "NUMERIC")
                }else{
                  types = types + (k -> "text") //if it is a scala object, perhaps it will be cast to text
                }
          }
          keys = keys ++ mp.keySet
             
         
         
         //if not exists, create it else look at columns and ensure that they exist
         if(!exists){
           this.createTable(table, keys.map { x => (x, types.get(x).get) })
         }else{
           keys.foreach { attribute => this.checkAndCreateColumn(table, attribute, types.get(attribute).get) }
         }
         
         
         if(data.get(table).get.size == 1){
            postMap(data.get(table).get(0),table)          
         }else{
           batchUpdate(data.get(table).get,table)
         }
     }
  }
  
  def postMappingsList():Unit={
    postMappingsList(mappingList)
    mappingList=scala.collection.immutable.Map[String,List[Map[String,Any]]]()
  }
  
  /**
   * Adds to a dataset which can be posted to the table. A batch update is called if the size exceeds the batch size.
   * 
   * @param  data                    A mapping list of the form Map[String,List[Map[String,Any]]] or Map[table,List[Map[column,data]]]
   * @param  table                   The table name to insert
   * @param  {Int} [batchsize]       The maximum size of the list data.
   * @param  {String}[hash]          Adds this hash to the records in the input map if present. Default is null.
   * @param  {String}[hashName]      A hash name to be used if the hash is not null. Default is "hash"
   */
  def addToMappingsList(data:List[Map[String,Any]],table:String,batchSize:Int = 100,hash:String = null,hashName:String = "hash") = {
    if(data.length > 0){
      var odata = data
      if(hash != null){
        for(i <- 0 until data.size){
          odata = odata.updated(i,odata(i) + (hashName -> hash))
        }
      }
      
      if(mappingList.contains(table)){
        mappingList = mappingList.updated(table, mappingList.get(table).get ++ odata)
      }else{
        mappingList = mappingList.updated(table, odata)
      }
      
      this.mapSize +=1 
      if(this.mapSize > batchSize){
        this.postMappingsList()
        mappingList = scala.collection.immutable.Map[String,List[Map[String,Any]]]()
        this.mapSize=0
      }
    }
  }
  
  /**
   * This function was made for my automated tool and requirement that data be kept for
   * x amount of time coupled with the compressive nature of PostgreSQL. Therefore,
   * Schema renames are necessary. Schema names are altered by appending a datestamp to 
   * them.
   * 
   * @param     schema    The schema to rename.
   * @return    The new schema name (old schema name + datestamp)
   */
  def renameSchema(schema:String):String={
    val newName = schema+String.valueOf(java.util.Calendar.getInstance.getTimeInMillis)
    val query = s"ALTER SCHEMA $schema RENAME TO $newName"
    this.update(query)
    newName
  }
  
  /**
   * Updates a completion table with the schema name
   * 
   * @param     completionTable         The table to insert completion data into.
   * @param     jobName                 The job name.
   * @param     schema                  The schema name.
   * @param     {Boolean[alterSchema]   Whether or not to alter the schema name. Default is true.         
   * @return    The final schema name.
   */
  def updateCompletionTable(completionTable:String,jobName:String,schema:String,alterSchema:Boolean = true):String={
    var finalSchema:String = schema
    if(alterSchema){
      finalSchema = this.renameSchema(finalSchema)
    }
    
    val query = s"INSERT INTO $completionTable  (name,schema) VALUES('$jobName','$finalSchema')"
    this.update(query)
    finalSchema
  } 
  
  /**
   * Disconnect from the database
   * @see Database#loadDbs
   */
  def disconnect()={
    DBs.closeAll()
  }
  
}