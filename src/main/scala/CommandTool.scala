
import scala.collection.mutable.ArrayBuffer
import scala.sys.process.{Process, ProcessLogger}

object CommandTool extends Logging {
  def runCommand(command: String, contextInfo: String = "running command") = {
    logDebug(s"Running command: [$command]")
    val outputBuffer = new ArrayBuffer[String]
    val processBuilder = Process(Seq("bash", "-c", command))
    val processLogger = ProcessLogger(outputBuffer += _)
    val exitCode = processBuilder.run(processLogger).exitValue()
    val output = outputBuffer.mkString("\n")
    logDebug(s"Output: [$output]")
    if (exitCode != 0) {
      throw new Exception(s"Error $contextInfo: command = [$command], output = \n$output")
    }
    (exitCode, output)
  }
}
