import java.nio.charset.Charset
import java.nio.file.{Files, Paths, StandardOpenOption}

import org.apache.commons.io.FileUtils
import scala.collection.JavaConversions.asJavaIterable

import CommandTool._
import SyncTool._


sealed trait TimeUnit
object MINUTES extends TimeUnit
object HOURS extends TimeUnit

case class Interval(time: Int, unit: TimeUnit)

case class SyncToolConf(sourceDirectory: String, destinationDirectory: String) {
  def toCommandString = s"$sourceDirectory $destinationDirectory"
}

/**
 * A tool that sets up copying of local log directory to a target S3 directory. It uses s3cmd and cron to set up
 * periodic syncing of local log directory to target S3 directory.
 * @param confDir            Path to the directory where configurations local and target directories will be stored
 * @param scriptFile         Path to where the script for syncing logs will be generated
 * @param interval           Syncing internal, should be of the same order as the log rollover interval
 * @param s3AccessKeyId      Access Key ID for accessing S3
 * @param s3SecretAccessKey  Secret Access Key for accessing S3
 */
class SyncTool(
    confDir: String,
    scriptFile: String,
    interval: Interval,
    s3AccessKeyId: String,
    s3SecretAccessKey: String
  ) extends Logging {
  val scriptFilePath = Paths.get(scriptFile).toAbsolutePath
  val confFilePath = Paths.get(confDir, CONF_FILE_NAME).toAbsolutePath
  val s3cmdConfFilePath = Paths.get(confDir, S3CMD_CONF_FILE_NAME).toAbsolutePath
  val syncCommand = s"$scriptFilePath $confFilePath $s3cmdConfFilePath"

  /** Initialize this tool by generating the script file and add a cron job. */
  def initialize() {
    logInfo("Initializing sync tool")
    generateScriptFile()
    configureCron()
    logInfo("Initialized sync tool")
  }

  /** Update configuration of this tool. */
  def updateConf(confs: Seq[SyncToolConf]) {
    generateConfFile(confs)
  }

  /** Run sync on the registered log directories. */
  def runSync() {
    logInfo("Running sync tool")
    runCommand(syncCommand, "running sync script")
  }

  /** Add cron job to run the script periodically. */
  private def configureCron() {
    logInfo("Configuration Cron")
    CronTool.addCommand(syncCommand, interval)
  }

  /** Generate the configuration file with the given log directories. */
  private def generateConfFile(confs: Seq[SyncToolConf]) {
    if (!Files.exists(confFilePath.getParent)) {
      Files.createDirectories(confFilePath.getParent)
    }
    if (Files.exists(confFilePath)) {
      logInfo("Running sync on previous sync conf file")
      runSync()
      Files.delete(confFilePath)
      logInfo("Deleted previous sync conf file")
    }

    logInfo(s"Generating sync conf file $confFilePath with ${confs.size} configs: $confs")
    val combinedConfString = confs.map(_.toCommandString).mkString(System.lineSeparator)
    logInfo("Generated sync conf string:\n" + combinedConfString)
    Files.write(confFilePath, asJavaIterable(combinedConfString.split("\n").toIterable), Charset.defaultCharset())
    logInfo("Generated sync conf file " + confFilePath.toAbsolutePath)
  }

  /** Generate the script file the sync local log directories to target S3 directories. */
  private def generateScriptFile() = {
    logInfo("Generating sync script")
    if (!Files.exists(scriptFilePath.getParent)) {
      Files.createDirectories(scriptFilePath.getParent)
    }

    // Generate s3cmd config file
    Files.deleteIfExists(s3cmdConfFilePath)
    val s3cmdConfigString = S3CMD_CONFIG_TEMPLATE.replace("$$ACCESS_KEY", s3AccessKeyId)
                                                 .replace("$$SECRET_KEY", s3SecretAccessKey)
    FileUtils.writeStringToFile(s3cmdConfFilePath.toFile, s3cmdConfigString)

    // Generate sync-tool.sh script
    Files.deleteIfExists(scriptFilePath)
    FileUtils.writeStringToFile(scriptFilePath.toFile, SCRIPT)
    runCommand(s"chmod +x $scriptFilePath", "setting S3 sync script to be executable")

  }
}

object SyncTool {

  val CONF_FILE_NAME = "sync-logs.conf"
  val S3CMD_CONF_FILE_NAME = "s3cmd.conf"
  val LOCK_FILE = "sync-log.lck"
  val SCRIPT =
    """
      |#!/usr/bin/env bash
      |
      |SCRIPT_NAME=`basename $BASH_SOURCE`
      |echo "====================================================="
      |echo "$SCRIPT_NAME called with arguments [$@]"
      |
      |if [[ "$#" -ne 2 ]] ; then
      |  echo "Usage: `basename $BASH_SOURCE` <config directory/file>  <s3cmd config file>"
      |  exit 1
      |fi
      |
      |# Get the configuration directory
      |CONF_DIR=$1
      |
      |S3CMD_CONF_FILE=$2
      |
      |# Check if s3cmd conf file exists
      |if [ -z $S3CMD_CONF_FILE ]; then
      |  echo "No s3cmd configuration file at $S3CMD_CONF_FILE"
      |  exit 1
      |fi
      |
      |# Get the configuration files
      |CONF_FILE_PATTERN="*sync-logs.conf"
      |CONF_FILES=`find $CONF_DIR -name $CONF_FILE_PATTERN`
      |if [ -z $CONF_FILES ]; then
      |  echo "No configuration file found that matches pattern $CONF_FILE_PATTERN"
      |  exit 0
      |else
      |  echo "Configuration files found: "
      |  echo $CONF_FILES
      |fi
      |
      |# Loop through the configuration files
      |for FILE in $CONF_FILES ; do
      |  echo Processing config file $FILE
      |  cat $FILE | while read LINE ; do
      |    [[ "$LINE" =~ ^#.*$ ]] && continue  # ignore comments
      |    [ -z "$LINE" ] && continue          # ignore empty lines
      |
      |    PARAMS=( $LINE )
      |    SOURCE_DIR=${PARAMS[0]}
      |    DEST_DIR=${PARAMS[1]}
      |
      |    # echo "Line: [$line]"
      |    # echo "Params: [${PARAMS[0]}] [${PARAMS[1]}]"
      |    # echo "Source directory: $SOURCE_DIR"
      |    # echo "Destination directory: $DEST_DIR"
      |
      |    # Check for existence of source and destination directories
      |    if [ -z "$SOURCE_DIR" ]; then
      |      echo "Source directory is empty in line '$LINE' in config file '$FILE'"
      |      exit 1
      |    fi
      |    if [ -z "$DEST_DIR" ]; then
      |      echo "Destination directory is empty in line '$LINE' in config file '$FILE'"
      |      exit 1
      |    fi
      |    if [ ! -e $SOURCE_DIR ]; then
      |      echo "Source directory '$SOURCE_DIR' from config file '$FILE' does not exist, skipping this."
      |      continue
      |    fi
      |
      |    # Sync the directories
      |    echo "Syncing from $SOURCE_DIR to $DEST_DIR"
      |    if [[ $DEST_DIR = s3* ]]; then
      |      s3cmd -c $S3CMD_CONF_FILE sync $SOURCE_DIR $DEST_DIR
      |    else
      |      rsync -trz $SOURCE_DIR $DEST_DIR
      |    fi
      |  done
      |done
      |
    """.stripMargin

  val S3CMD_CONFIG_TEMPLATE =
    """
      |[default]
      |access_key = $$ACCESS_KEY
      |add_encoding_exts =
      |add_headers =
      |bucket_location = US
      |cache_file =
      |cloudfront_host = cloudfront.amazonaws.com
      |default_mime_type = binary/octet-stream
      |delay_updates = False
      |delete_after = False
      |delete_after_fetch = False
      |delete_removed = False
      |dry_run = False
      |enable_multipart = True
      |encoding = UTF-8
      |encrypt = False
      |follow_symlinks = False
      |force = False
      |get_continue = False
      |gpg_command = None
      |gpg_decrypt = %(gpg_command)s -d --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
      |gpg_encrypt = %(gpg_command)s -c --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
      |gpg_passphrase =
      |guess_mime_type = True
      |host_base = s3.amazonaws.com
      |host_bucket = %(bucket)s.s3.amazonaws.com
      |human_readable_sizes = False
      |invalidate_default_index_on_cf = False
      |invalidate_default_index_root_on_cf = True
      |invalidate_on_cf = False
      |list_md5 = False
      |log_target_prefix =
      |mime_type =
      |multipart_chunk_size_mb = 15
      |preserve_attrs = True
      |progress_meter = True
      |proxy_host =
      |proxy_port = 0
      |recursive = False
      |recv_chunk = 4096
      |reduced_redundancy = False
      |secret_key = $$SECRET_KEY
      |send_chunk = 4096
      |simpledb_host = sdb.amazonaws.com
      |skip_existing = False
      |socket_timeout = 300
      |urlencoding_mode = normal
      |use_https = False
      |verbosity = WARNING
      |website_endpoint = http://%(bucket)s.s3-website-%(location)s.amazonaws.com/
      |website_error =
      |website_index = index.html
    """.stripMargin.split(System.lineSeparator).filter(_.size > 0).mkString(System.lineSeparator)
}







