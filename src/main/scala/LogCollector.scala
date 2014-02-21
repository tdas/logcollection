import java.io.{File, RandomAccessFile}
import java.nio.file.{Files, Paths}

import scala.collection.mutable.HashMap
import scala.concurrent._

import ExecutionContext.Implicits.global
import LogCollector._

/**
 * A tool that allows a local log directory to be collected and saved to S3.
 * The local log directory and the target S3 log directory has to be registered with instances
 * of this class.
 *
 * @param confDir            The directory where the configuration related to this log collector is going to be stored.
 * @param s3AccessKeyId      Access Key ID for accessing S3
 * @param s3SecretAccessKey  Secret Access Key for accessing S3
 * @param syncToolScriptFile This optional parameter allows you to specify the location of the sync tool script file.
 *                           By default the location is specified by LogCollector.DEFAULT_SYNC_TOOL_SCRIPT_FILE
 *                           which should be set to a common file path across all instances of LogCollectors.
 * @param syncToolInterval   This optional parameter specifies the sync interval. By default is specified by
 *                           LogCollect.DEFAULT_SYNC_TOOL_INTERVAL.
 *
 * Note that if the syncToolScriptFile and/or syncToolInterval are specified when creating
 * an instance of LogCollector then it will be tied to the current implementation.
 */
class LogCollector(
    confDir: String,
    s3AccessKeyId: String,
    s3SecretAccessKey: String,
    syncToolScriptFile: String = DEFAULT_SYNC_TOOL_SCRIPT_FILE,
    syncToolInterval: Interval = DEFAULT_SYNC_TOOL_INTERVAL
  ) extends Logging {

  case class LogSource(name: String, logDirectory: String, targetLogDirectory: String) {
    def toS3SyncConf = new SyncToolConf(logDirectory + File.separator, targetLogDirectory)
  }

  val syncTool = new SyncTool(confDir, syncToolScriptFile, syncToolInterval, s3AccessKeyId, s3SecretAccessKey)
  val sources = new HashMap[String, LogSource]

  /** Initialize the log collector by generating necessary scripts, etc. */
  def initialize() = fileSynchronized {
    logInfo("Initializing log collector")
    syncTool.initialize()
    updateConf()
    logInfo("Initialized log collector")
  }

  /** Register a new logging source for collection and syncing to S3. */
  def register(name: String, logDirectory: String, targetLogDirectory: String) = fileSynchronized {
    logInfo(s"Registering log source $name")
    sources(name) = LogSource(name, logDirectory, targetLogDirectory)
    updateConf()
    logInfo(s"Registered log source $name")
  }

  /** Deregister an existing logging source. */
  def deregister(name: String) = fileSynchronized {
    logInfo(s"Deregistering log source $name")
    sources -= name
    updateConf()
    logInfo(s"Deregistered log source $name")
  }

  /** Update configuration of internal tools when registered log sources change. */
  private def updateConf() {
    syncTool.updateConf(sources.values.map(_.toS3SyncConf).toSeq)
  }
}

/**
 * Companion object of LogCollector containing a few defaults and utility functions.
 */
object LogCollector {

  /**
   * The default location of the sync tool script. Its best to keep this at some system-wide common
   * location so that all instances of LogCollector across all JVMs uses the same script. May make it
   * easier to be manage.
   */
  private val DEFAULT_SYNC_TOOL_SCRIPT_FILE = Paths.get(System.getProperty("user.home"), "sync-tool.sh").toString

  /**
   * The default interval at which the syncing script will be executing by cron.
   */
  private val DEFAULT_SYNC_TOOL_INTERVAL = Interval(1, HOURS)

  /**
   * The lock file that is used to ensure that multiple instances (in multiple JVMs) of this tool
   * do not modify make system-wide changes (like generation of the scripts, etc.) simultaneously.
   *
   * @note CHANGE THIS TO A SYSTEM-WIDE-COMMON DIRECTORY IN THE DATABRICKS AMI.
   */
  private val LOCK_FILE = Paths.get(System.getProperty("user.home"), ".log-collector.lck").toString

  /**
   * Synchronizes the execution of all LogCollection instances across all JVMs.
   */
  private def fileSynchronized[A](body: => A): A = synchronized {
    fileSynchronized[A](LOCK_FILE)(body)
  }

  /**
   * Utility function that allows components in multiple JVMs to synchronize their execution while performing
   * system-wide changes that affect all those components.
   */
  def fileSynchronized[A](lockFilePath: String)(body: => A): A = {
    import scala.concurrent.duration._
    var lockFile: RandomAccessFile = null
    try {
      val lockFileAbsPath = Paths.get(lockFilePath).toAbsolutePath
      lockFile = new RandomAccessFile(lockFileAbsPath.toString, "rw")
      val lockFuture = future {
        val lock = lockFile.getChannel.lock()
        try {
          body
        } finally {
          lock.release()
        }
      }
      Await.result(lockFuture, 2 seconds)
    } finally {
      if (lockFile != null) {
        lockFile.close()
      }
      Files.deleteIfExists(Paths.get(lockFilePath))
    }
  }

  def main(args: Array[String]) {
    if (args.size < 3) {
      println("Usage: " + this.getClass.getSimpleName.stripSuffix("$") +
        " <conf directory>  <local log directory>  <target S3 directory>")
      System.exit(1)
    }
    val Array(confDir, localLogDir, targetS3Dir) = args
    val lc = new LogCollector(confDir, System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY"))
    lc.register("test", localLogDir, targetS3Dir)
    lc.initialize()
    Thread.sleep(10000000)
  }
}
