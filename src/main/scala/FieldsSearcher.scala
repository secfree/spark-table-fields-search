import org.apache.commons.cli.{CommandLine, HelpFormatter, Options, PosixParser}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Column

import scala.collection.mutable
import scala.util.matching.Regex

object FieldsSearcher {
  var db: String = _
  var recursive: Boolean = false

  def main(args: Array[String]): Unit = {
    val cmd = parseArgs(args) match {
      case Some(p) => p
      case None => sys.exit(-1)
    }
    db = cmd.getOptionValue("database")
    println("Searching in database: " + db)

    val fields = cmd.getOptionValue("fields").split(",").toSet
    println("Searching fields: " + fields.toString())

    if (cmd.hasOption("recursive")) {
      recursive = true
    }

    if (cmd.hasOption("regex")) {
      regexSearch(fields)
    } else {
      search(fields)
    }
  }

  /**
    * Parse command line options.
    *
    * @param args command line args
    * @return
    */
  def parseArgs(args: Array[String]): Option[CommandLine] = {
    val options = new Options()
    options.addOption("database", true, "database")
    options.addOption("fields", true, "fields separated by ','")
    options.addOption("regex", false, "regex mode")
    options.addOption("recursive", false, "search 'struct' type fields recursively")
    options.addOption("h", "help", false, "print help information")
    val parser = new PosixParser()

    try {
      val cmd = parser.parse(options, args)

      if (cmd.hasOption("h") || cmd.hasOption("help")) {
        val help = new HelpFormatter()
        help.printHelp("Tools", options)
        return None
      }

      val needArgs = List("database", "fields")
      for (a <- needArgs) {
        if (!cmd.hasOption(a)) {
          sys.error("Option '" + a + "' must be supplied!")
          return None
        }
      }
      Some(cmd)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.error("Parse command line options failed!")
        None
    }
  }

  /**
    * Get the Spark session
    * @param name application name
    * @return
    */
  def getSparkSession(name: String): SparkSession = {
    SparkSession.builder().appName(name).enableHiveSupport().getOrCreate()
  }

  /**
    * Search tables which contain all fields
    * @param fields fields in plain text format
    */
  def search(fields: Set[String]): Unit = {
    val spark = getSparkSession(getClass.toString)
    val tables = spark.catalog.listTables(db).collect()
    for (t <- tables) {
      try {
        val cols = spark.catalog.listColumns(db, t.name).collect()
        var colNames = new mutable.HashSet[String]()
        for (c <- cols) {
          colNames = colNames.union(getColumnNames(c))
        }
        if (fields.subsetOf(colNames)) {
          println("Found match table: " + t)
        }
      } catch {
        case e: Exception =>
          sys.error(e.toString)
      }
    }
  }

  /**
    * Search tables which match all fields with regex pattern
    * @param fields fields in regex pattern format
    */
  def regexSearch(fields: Set[String]): Unit = {
    val spark = getSparkSession(getClass.toString)
    val tables = spark.catalog.listTables(db).collect()
    val patterns = new mutable.HashSet[Regex]()
    for (field <- fields) {
      patterns.add(new Regex(field))
    }

    for (t <- tables) {
      try {
        val cols = spark.catalog.listColumns(db, t.name).collect()
        var colNames = new mutable.HashSet[String]()
        for (c <- cols) {
          colNames = colNames.union(getColumnNames(c))
        }
        if (isRegexSubSetOf(patterns.toSet, colNames.toSet)) {
          println("Found match table: " + t)
        }
      } catch {
        case e: Exception =>
          sys.error(e.toString)
      }

    }
  }

  /**
    * Get names in a column
    * @param column the Column value
    * @return
    */
  def getColumnNames(column: Column): Set[String] = {
    if (!recursive || !column.dataType.startsWith("struct<")) {
      return Set[String](column.name)
    }

    val fields = column.dataType.substring(7, column.dataType.length - 1).split(",")
    val colNames = new mutable.HashSet[String]()
    colNames.add(column.name)
    for (f <- fields) {
      colNames.add(f.split(":")(0))
    }
    colNames.toSet
  }

  /**
    * Check if values match all patterns
    * @param patterns regex patterns
    * @param values value set
    * @return
    */
  def isRegexSubSetOf(patterns: Set[Regex], values: Set[String]): Boolean = {
    for (p <- patterns) {
      if(!isContainPattern(p, values)) {
        return false
      }
    }
    true
  }

  /**
    * Check if one value matches the regex pattern
    * @param pattern regex pattern
    * @param values value set
    * @return
    */
  def isContainPattern(pattern: Regex, values: Set[String]): Boolean = {
    for (v <- values) {
      val result = pattern.findFirstIn(v)
      if (result.nonEmpty) {
        return true
      }
    }
    false
  }
}
