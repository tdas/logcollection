import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuite}
import scala.collection.JavaConversions._

class LogCollectorSuite extends FunSuite with BeforeAndAfter with BeforeAndAfterAll {

  val s3AccessKeyId = "ABC"
  val s3SecretAccessKey = "XYZ"
  val currentDir = Paths.get(System.getProperty("user.dir")).toAbsolutePath

  // Required for log collector
  val confDir = Paths.get(currentDir.toString, "conf")
  val syncToolScriptFile = Paths.get(currentDir.toString, "sync-logs.sh")

  // Log directory for testing
  val logDir = Paths.get(currentDir.toString, "log" +  File.separator).toAbsolutePath
  val targetLogDir = Paths.get(currentDir.toString, "log-target").toAbsolutePath

  before {
    FileUtils.deleteDirectory(confDir.toFile)
    Files.deleteIfExists(syncToolScriptFile)
    FileUtils.deleteDirectory(logDir.toFile)
    FileUtils.deleteDirectory(targetLogDir.toFile)
    CronTool.clearAllCommands()
  }

  override def afterAll() {
    FileUtils.deleteDirectory(confDir.toFile)
    Files.deleteIfExists(syncToolScriptFile)
    FileUtils.deleteDirectory(logDir.toFile)
    FileUtils.deleteDirectory(targetLogDir.toFile)
    CronTool.clearAllCommands()
  }

  test("CronTool - adding and removing commands") {
    val command = "ls ~/"

    // Clear all commands
    assert(CronTool.getCurrentCommands.isEmpty)

    // Add the command
    CronTool.addCommand(command, Interval(5, MINUTES))
    assert(CronTool.getCurrentCommands.size === 1)
    assert(CronTool.getCurrentCommands.head.contains("*/5 * * * *"))
    assert(CronTool.getCurrentCommands.head.contains(command))
    assert(CronTool.isCommandPresent(command))

    // Remove the command
    CronTool.removeCommand(command)
    assert(!CronTool.isCommandPresent(command))

    // Replace the command
    CronTool.addCommand(command, Interval(5, MINUTES))
    CronTool.addCommand(command, Interval(10, HOURS))
    assert(CronTool.getCurrentCommands.size === 1,
      "Commands present are: " + CronTool.getCurrentCommands)
    assert(CronTool.getCurrentCommands.head.contains("* */10 * * *"))
    assert(CronTool.getCurrentCommands.head.contains(command))
    assert(CronTool.isCommandPresent(command))

    // Clear all commands
    CronTool.clearAllCommands()
    assert(CronTool.getCurrentCommands.isEmpty)

    // Do not replace the command
    CronTool.addCommand(command, Interval(5, MINUTES))
    CronTool.addCommand(command, Interval(10, HOURS), false)
    assert(CronTool.getCurrentCommands.size === 2)
    val currentCommands = CronTool.getCurrentCommands.sorted
    assert(currentCommands(0).contains("* */10 * * *"))
    assert(currentCommands(1).contains("*/5 * * * *"))

    CronTool.clearAllCommands()
  }

  test("SyncTool - script generation, config generation, and syncing") {
    assert(!Files.exists(confDir))
    assert(!Files.exists(logDir))
    assert(!Files.exists(targetLogDir))
    assert(!Files.exists(syncToolScriptFile))

    // Script generation
    val syncTool = new SyncTool(
      confDir.toString, syncToolScriptFile.toString, Interval(1, MINUTES), s3AccessKeyId, s3SecretAccessKey)
    syncTool.initialize()
    // assert(Files.exists(confDir)) gives compilation error
    // [Such types can participate in value classes, but instances
    // cannot appear in singleton types or in reference comparisons.]
    assert(Files.exists(syncToolScriptFile) === true)
    assert(Files.readAllLines(syncToolScriptFile, Charset.defaultCharset).mkString("\n") === SyncTool.SCRIPT)
    assert(Files.exists(syncTool.s3cmdConfFilePath) === true)
    val s3cmdConfFileContents = Files.readAllLines(syncTool.s3cmdConfFilePath, Charset.defaultCharset()).mkString("\n")
    assert(s3cmdConfFileContents.contains(s3AccessKeyId))
    assert(s3cmdConfFileContents.contains(s3SecretAccessKey))
    assert(CronTool.isCommandPresent(syncToolScriptFile.toString))


    // Write config
    syncTool.updateConf(Seq(SyncToolConf(logDir.toString + File.separator, targetLogDir.toString)))
    assert(Files.exists(confDir) === true)
    assert(Files.isDirectory(confDir) === true)
    assert(Files.exists(syncTool.confFilePath) === true)
    val confFileContents = Files.readAllLines(syncTool.confFilePath, Charset.defaultCharset)
                                .filter(_.size > 0)
    assert(confFileContents.size === 1)
    val confParts = confFileContents.head.split(" ")
    assert(confParts.size === 2)
    assert(confParts(0).contains(logDir.toString))
    assert(confParts(1).contains(targetLogDir.toString))

    // Sync log directory
    Files.createDirectories(logDir)
    Files.createDirectories(targetLogDir)
    val newLogFile = Paths.get(logDir.toString, "test.log")
    assert(!Files.exists(newLogFile))
    Files.write(newLogFile, new Array[Byte](100))
    assert(Files.exists(newLogFile) === true)
    syncTool.runSync()
    val syncedLogFile = Paths.get(targetLogDir.toString, newLogFile.getFileName.toString)
    assert(Files.exists(syncedLogFile) === true)
  }

  test("LogCollector - initialization, registration, deregistration") {
    assert(!Files.exists(confDir))
    assert(!Files.exists(logDir))
    assert(!Files.exists(targetLogDir))
    assert(!Files.exists(syncToolScriptFile))

    // Initialize and register
    val logCollector = new LogCollector(confDir.toString, "", "", syncToolScriptFile.toString)
    logCollector.initialize()
    assert(CronTool.isCommandPresent(syncToolScriptFile.toString))
    logCollector.register("test", logDir.toString , targetLogDir.toString)
    assert(Files.exists(logCollector.syncTool.confFilePath) === true)
    val confFileContent = Files.readAllLines(logCollector.syncTool.confFilePath, Charset.defaultCharset).head
    assert(confFileContent.contains(logDir.toString))

    // Sync log directory
    Files.createDirectories(logDir)
    Files.createDirectories(targetLogDir)
    val newLogFile = Paths.get(logDir.toString, "test.log")
    assert(!Files.exists(newLogFile))
    Files.write(newLogFile, new Array[Byte](100))
    assert(Files.exists(newLogFile) === true)
    logCollector.syncTool.runSync()
    val syncedLogFile = Paths.get(targetLogDir.toString, newLogFile.getFileName.toString)
    assert(Files.exists(syncedLogFile) === true)

    // Deregister and sync log directory
    logCollector.deregister("test")
    assert(Files.readAllLines(logCollector.syncTool.confFilePath, Charset.defaultCharset).filter(_.size > 0).isEmpty)
    val newLogFile1 = Paths.get(logDir.toString, "test1.log")
    assert(!Files.exists(newLogFile1))
    Files.write(newLogFile1, new Array[Byte](100))
    assert(Files.exists(newLogFile1) === true)
    logCollector.syncTool.runSync()
    val syncedLogFile1 = Paths.get(targetLogDir.toString, newLogFile1.getFileName.toString)
    assert(!Files.exists(syncedLogFile1) === true)
  }
}
