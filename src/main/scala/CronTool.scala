
import scala.util.{Failure, Success, Try}

import CommandTool.runCommand

object CronTool extends Logging {

  private val LOCK_FILE = ".cron-tool.lck"

  /** Get current commands in crontab */
  def getCurrentCommands: Seq[String] = fileSynchronized {
    getCommands
  }

  /**
   * Insert a command in crontab.
   * @param command: Bash command
   * @param interval: Time interval after which the command will be executed
   * @param replaceExisting: Whether to replace the command, if it already exists
   */
  def addCommand(
      command: String,
      interval: Interval,
      replaceExisting: Boolean = true
    ) = fileSynchronized {

    val Interval(time, unit) = interval
    val timing = unit match {
      case MINUTES => s"*/$time * * * *"
      case HOURS =>   s"* */$time * * *"
      case _ => throw new Exception("Unrecognized unit")
    }
    val currentTabs = getCommands
    val newTab = s"$timing     $command"
    logDebug(s"New tab $newTab")

    val filteredTabs = if (replaceExisting) {
      currentTabs.filter(!_.contains(command)).filter(_.size > 0)
    } else {
      currentTabs
    }
    logDebug(s"Filtered tabs (${filteredTabs.size}): $filteredTabs")
    val updatedTabs = filteredTabs ++ Seq(newTab)

    Try {
      runCommand("echo \"" + updatedTabs.mkString(System.lineSeparator()) + "\" | crontab -")
    } match {
      case Success(_) => logInfo(s"Added command $command to crontab")
      case Failure(ex) => logError(s"Error adding command $command to crontab: " + ex)
    }
  }

  /** Delete the command if it exists in crontab. */
  def removeCommand(command: String) = fileSynchronized {
    val currentTabs = getCommands
    logDebug("Current tabs:\n" + currentTabs.mkString("\n"))
    val filteredTabs = currentTabs.filter(!_.contains(command))
    logDebug("Filtered tabs:\n" + filteredTabs.mkString("\n"))

    if (filteredTabs.size != currentTabs.size) { // something was removed
      Try {
        runCommand("echo \"" + filteredTabs.mkString(System.lineSeparator()) + "\" | crontab -")
      } match {
        case Success(_) => logInfo(s"Removed command [$command] from crontab")
        case Failure(ex) => logError(s"Error deleting command [$command] from crontab: " + ex)
      }
    } else {
      logInfo(s"Not found command [$command], nothing to delete from crontab")
    }
  }

  /** Clear all commands in crontab. */
  def clearAllCommands() = fileSynchronized {
    Try {
      runCommand("crontab -r")
    } match {
      case Success(output) =>
        logInfo("Cleared crontab")
      case Failure(ex) =>
        logError("Error getting current cron tabs: " + ex)
        Seq.empty
    }
  }

  /** Check if a command is present or not */
  def isCommandPresent(command: String) = fileSynchronized {
    !getCommands().forall(!_.contains(command))
  }

  /** Get the commands from crontab. */
  private def getCommands(): Seq[String] = {
    Try {
      runCommand("crontab -l")
    } match {
      case Success(output) =>
        output._2.split(System.lineSeparator).filter(_.size > 0).toSeq
      case Failure(ex) =>
        logError("Error getting current cron tabs: " + ex)
        Seq.empty
    }
  }

  /**
   * Synchronizes the execution of all CronTool instances across all JVMs.
   */
  def fileSynchronized[A](body: => A): A = synchronized {
    LogCollector.fileSynchronized[A](LOCK_FILE)(body)
  }
}
