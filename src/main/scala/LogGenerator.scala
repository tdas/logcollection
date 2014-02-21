object LogGenerator extends Logging {
  def main(args: Array[String]) {
    while(true) {
      logInfo(s"Hello World!")
      Thread.sleep(50)
    }
  }
}
