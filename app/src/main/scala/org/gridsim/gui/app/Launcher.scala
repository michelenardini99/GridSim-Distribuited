package org.gridsim.gui.app

/** Launcher class for the GridSim GUI application.
  *
  * This class does not extend {@link scalafx.application.JFXApp3} or
  * {@link javafx.application.Application}, which allows the application to be
  * launched from a fat/uber JAR on the classpath without triggering JVM-level
  * JavaFX module system checks.
  */
object Launcher:
  def main(args: Array[String]): Unit =
    if (args.headOption.contains("backend")) {
      org.gridsim.actor.ActorMain.main(args.tail)
    } else if (args.headOption.contains("gui")) {
      GuiApp.main(args.tail)
    } else {
      println("Usage: ./gradlew run --args=\"[backend|gui] ...\"")
      println("Running GUI by default...")
      GuiApp.main(args)
    }
