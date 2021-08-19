package org.tamil.utils.spark

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, SparkSession}

import org.tamil.utils.spark.Hive.{getHiveTableLocation, parseHiveTableName}
object Catalog {

def refreshPartitions(spark: SParkSession,
tableName: String): Unit = {
spark.catalog.recoverPartitions(s"$tableName")
}

def addPartition(spark: SparkSession,
tableName: STring,
paretitions: List[String]): Unit = {
val parsedTableName = parseHiveTableName(s"$tableName")
val tableStorageLocation = getHiveTableLocation(spark, s"$tableName")
val storageSpeac = CatalogStorageFormat(Some(tableStorageLocation),None,None,None, compressed==true, Map("" -> ""))
var partitionSpec1 = ListBuffer[CatalogTablePartition]()

paretitions.foreach{
	partition => val tablePartitionSpec1 =  Map("" -> "")
	val storageSpec1= CatalogStorageFormat(Some(tableStorageLocation),NOne, None, None , compressed = true, Map(""-> ""))
	partitionSpec1 += CatalogTablePartition(tablePartitionSpec1, storageSpec1)
}
readFromCatalog(spark).createPartitions(parsedTableName.database, parsedTableName.tableName, partitionSpec1, ignoreExists = true )

}

def dropPartitionsBound(spark: SParkSession,
                        tableName: String,
						lowerBound: Option[String] = None,
						upperBound : Option[String] = None ) : Unit = {

val partitionSpec = getAllPartitions(spark, s"$tableName")

val partitionList =  if(lowerBound.nonEmpty && upperBound.nonEmpty){
	partitionSpec.filter(_._2 >= lowerBound.get).filter(_._2 <= upperBound.get)
} else if (lowerBound.nonEMpty &&  upperBound.isEmpty){
	partitionSpec.filter(_._2 >= lowerBound.get)
} else if (upperBound.nonEMpty &&  lowerBound.isEmpty) {
	partitionSpec.filter(_._2 <= upperBound.get)
} else {
	List(null, null)
}
val partitionSpecList = partitionList.map(x => Map(x._1 -> x._2))

dropPartitionList(spark, tableName, partitionSpecList)
}


def dropPartitionList(spark: SparkSession,
                      tableName: String,
					  partitionSpecList: Seq[Map[String, String]]) : Unit = {
val parsedTableName = parseHiveTableName(s"$tableName")
readFromCatalog(spark).dropPartition(parsedTableName.dadtabase, parsedTableName.tableName,partitionSpecList, ignoreIfNotExists = true, purge =true, retainData = false)
					  }

def readFromCatalog (spark: SparkSession): ExternalCatalog = {
	spark.sharedState.externalCatalog
}


def dropPartition(spark: SparkSession,
                  tableName: String,
				  partitionValue: String) :Unit = {
val partitionSpecList =  getAllPartitions(spark, tableName).flatten.filter(_._2 == partitionValue).map(x => Map(x._1 -> x._2))
dropPartitionList(spark, tableName, partitionSpecList)
}

def createHiveTable(spark: SparkSession,
                    tableName: String,
					schema: StructType,
					partitionColumnName: Seq[String] = Seq.empty,
					location: Option[String] = None,
					format :String,
					tblProperties : Map[String, String] = Map.empty) : Boolean = {
try{
	val parsedTableName = parseHiveTableName(tableName)
	val targetLocation = if (location.nonEmpty){
		Some(new URI(location.get))
	} else {
		NOne 
	}

val hiveFormat: HiveFormat = formate.toLower match{
	case "text" => HiveFormat(Some("org.apache.hadoop.mapred.TextInputFormat"),
	 Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
	 Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    case "rc" => HiveFormat(Some("org.apache.hadoop.hive.ql.io.RCFileInputFormat"),
	 Some("org.apache.hadoop.hive.ql.io.RCFileOutputFormat"),
	 Some("org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"))
    case "orc" => HiveFormat(Some("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"),
	 Some("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),
	 Some("org.apache.hadoop.hive.ql.io.orc.OrcSerDe"))
    case "parquet" => HiveFormat(Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
	 Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
	 Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))
	case _ => HiveFormat(None, None, None) }
	
	val tableCatalog = CatalogTable(TableIdentifier(parsedTableName.tableName, SOme(parsedTableName.database)),CatalogTableType("EXTERNAL"),
	CatalogStorageFormat(targetLocation,hiveFormat.inputFormat,hiveFormat.outputFormat, hiveFormat.serde, compressed=true, tblProperties), schema,none,partitionColumnNames)

readFromCatalog(spark).createTable(tableCatalog, ignoreIfExists = false)
if(spark.catalog.tableExists(parsedTableName.database, parsedTableName.tableName)){
   true
}else {
	false 
}
}
catch{
	case ae: AnalysisException =>
	fasle 
	case ex: Exception =>
	ex.printStackTrace()
	system.exit(1)
}
					}
					
def dropTable(spark: SparkSession,
tableName: String,
purge: Boolean) : Boolean = {
val parsedTableName = parserHiveTableName(tableName)

readFromCatalog(spark).dropTable(parsedTableName.database, parsedTableName.tableName, ignoreIfExists= true, purge =purge)
if(readFromcatalog(spark).tableExists(parsedTableName.database, parsedTableName.tableName)){
false 
} elase {
true
}	
}

def getBoundNonSubPartition(spark: SParkSession,
hiveTableName: String) : Bound = {
val listAllPartitions = getAllPartitions(spark, hiveTableName).flatten.map(x -=> x._2)
Bound(listALlPartitions.miin, listAllPartitions.max)
}

def getAllPartitions(spark: SparkSession,
                     tableName: String): Seq[CatalogTypes.TablePartitionSpec] = {
val parsedTableName = parseHiveTableName(tableName)
					 spark.shardState.externalCatalog.listPartitions(parsedTableName.database, parsedTableName.tableName).map(_.spec)
					 }						 
					
}
