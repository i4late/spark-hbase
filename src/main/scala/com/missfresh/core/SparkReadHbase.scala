package com.missfresh.core


import com.missfresh.utils.SpeedyConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.CustomClassLoaderConstructor

/**
 * 需将core-site.xml&hdfs-site.xml&hbase-site.xml & hive-site.xml 文件放在资源目录resources下
 */
object SparkReadHbase {
  def main(args: Array[String]): Unit = {
    assert(args.size == 1, "需要指定配置文件名称")
    val stream = getClass().getClassLoader().getResourceAsStream(args(0))
    //val stream = ClassLoader.getSystemResourceAsStream(args(0))
    //val yaml = new Yaml(new Constructor(classOf[com.missfresh.utils.SpeedyConfig]))
    val yaml = new Yaml(new CustomClassLoaderConstructor(classOf[SpeedyConfig], Thread.currentThread.getContextClassLoader))
    val config: SpeedyConfig = yaml.load(stream).asInstanceOf[com.missfresh.utils.SpeedyConfig]
    assert(null != config.getObjectVO, "需要同步表的schema为空")
    assert(null != config.getJobName, "spark 作业名称为空")
    assert(null != config.getColFamily, "hbase cf 为空")
    assert(null != config.getSnapshotPath, "snapshotPath 为空")
    assert(null != config.getSnapshotFile, "snapshot file 为空")
    assert(null != config.getTargetTable, "目标表 为空")
    val spark = SparkSession.builder()
      .appName(config.getJobName)
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    // 获取HbaseRDD
    val job = Job.getInstance(getHbaseConf())
    TableSnapshotInputFormat.setInput(job, config.getSnapshotFile, new Path(config.getSnapshotPath))

    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(job.getConfiguration,
      classOf[TableSnapshotInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //动态转换
    import scala.collection.JavaConverters._
    val colArray = config.getObjectVO.asScala.toList.map(tu => StructField(tu._1, getSparkType(tu._2), true)).toArray
    val schema = StructType(colArray)
    val result = hbaseRDD.map(_._2).map(getRes(_, config.getColFamily, schema.toList))
    val dataDF = spark.createDataFrame(result, schema)
    dataDF.createOrReplaceTempView("hbase_" + config.getSnapshotFile)
    val tabAllColArray = spark.table(config.getTargetTable).columns
    val tmpColArray = config.getObjectVO.keySet().toArray()
    val diffCol = tabAllColArray.diff(tmpColArray)
    println("目标表字段: " + tabAllColArray.mkString(","))
    println("配置字段: " + tmpColArray.mkString(","))
    println("目标表字段数: " + tabAllColArray.size + " ,配置字段数: " + tmpColArray.size + " ,相差字段: [" + diffCol.mkString(",") + " ]")
    if (diffCol.size > 0) {
      spark.stop()
      System.exit(1)
    }
    val sql = "INSERT OVERWRITE TABLE  " + config.getTargetTable + "  select " + tabAllColArray.mkString(",") + " from hbase_" + config.getSnapshotFile
    println("执行sql 为:" + sql)
    spark.sql(sql)
    spark.stop()
  }

  def getSparkType(colType: String): DataType = colType.toLowerCase() match {
    case "string" => StringType
    case "tinyint" | "int" => IntegerType
    case "bigint" => LongType
    // case "tinyint" =>BooleanType //需要看hive数据
    case u if u.startsWith("decimal") => DoubleType
  }

  def getRes(result: Result, colFamily: String, colList: List[StructField]): Row = {
    val rowkey = Bytes.toString(result.getRow())
    val hbaseRow = colList.map(o => {
      o.dataType match {
        case StringType => Bytes.toString(result.getValue(colFamily.getBytes, o.name.getBytes))
        case IntegerType => Integer.valueOf(Bytes.toString(result.getValue(colFamily.getBytes, o.name.getBytes)))
        case LongType => Bytes.toLong(result.getValue(colFamily.getBytes, o.name.getBytes))
        case DoubleType => Bytes.toDouble(result.getValue(colFamily.getBytes, o.name.getBytes))
      }
    })
    //rowkey 添加到list 的前面
    Row.fromSeq(rowkey +: hbaseRow)
  }

  // 构造 Hbase 配置信息
  def getHbaseConf(): Configuration = {
    val conf: Configuration = HBaseConfiguration.create()
    conf.set(TableInputFormat.SCAN, getScanStr())
    conf
  }

  // 获取扫描器
  def getScanStr(): String = {
    val scan = new Scan()
    // scan.set....  各种过滤
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray())
  }

}
