package org.gridsim.gui.view

import org.gridsim.gui.viewmodel.{SimulationCoordinator, SimulationViewLayout}
import scalafx.scene.Parent
import scalafx.scene.control.{Label, SplitPane, Tab, TabPane}
import scalafx.scene.layout.{BorderPane, Priority, StackPane, VBox}

import org.gridsim.gui.view.layouts.{
  SimulationLayoutStrategy,
  SplitSimulationLayout,
  TabsSimulationLayout
}
import scalafx.application.Platform

/** Main view layout for the active simulation screen.
  *
  * This composite view organizes the scenario title, grid visualization,
  * selected entity details, statistics, and simulation controls.
  *
  * @param coordinator
  *   the coordinator that manages state orchestration across the view
  *   components
  */
class SimulationView(val coordinator: SimulationCoordinator)
    extends BorderPane
    with ViewFX:
  override def root: Parent = this

  private val scenarioTitle = new Label(coordinator.scenarioName):
    styleClass ++= Seq("title", "simulation-title", "main-title")

  private val controlView = new SimulationControlView(
    coordinator.controlViewModel
  )

  private val graphView = GraphView(coordinator)
  private val statsView = StatisticsView(coordinator)

  private val stack = new StackPane

  private val centerContent = new VBox:
    children = Seq(scenarioTitle, stack, controlView)

  VBox.setVgrow(stack, Priority.Always)
  center = centerContent

  private def refreshContentLayout(): Unit =
    Platform.runLater {
      stack.delegate.applyCss()
      stack.delegate.requestLayout()
      stack.delegate.layout()
      centerContent.delegate.requestLayout()
      delegate.requestLayout()
      javafx.application.Platform.requestNextPulse()
    }

  // A TabPane lays out its selected content through its skin. Changes made
  // inside the details ScrollPane do not always invalidate that upper layout
  // on the first selection, while the split layout does not have this extra
  // layer. Refresh the complete simulation area when the details model changes
  // so Tab and Split layouts behave consistently.
  coordinator.entityDetailsViewModel.detailsEntityProperty.onChange {
    (_, _, _) => refreshContentLayout()
  }

  // Statistics can change while their tab is already selected. In that case
  // selectedItem does not change, so refresh directly after every stats update.
  coordinator.onStatisticsUpdated = () => refreshContentLayout()
  refreshContentLayout()

  def updateLayout(layout: SimulationViewLayout): Unit =
    val strategy: SimulationLayoutStrategy = layout match
      case SimulationViewLayout.Tabs  => new TabsSimulationLayout()
      case SimulationViewLayout.Split => new SplitSimulationLayout()

    stack.children = Seq(
      strategy.build(graphView.graphArea, statsView.statisticsArea)
    )

  coordinator.controlViewModel.detailsLayout.onChange { (_, _, layout) =>
    updateLayout(layout)
  }

  // Initial layout setup
  updateLayout(coordinator.controlViewModel.detailsLayout.value)
