package org.gridsim.gui.view

import org.gridsim.gui.ports.{SimulationApiClient, SimulationItem}
import scalafx.Includes._
import scalafx.scene.layout.{VBox, HBox, Priority}
import scalafx.scene.control.{Button, Label, ListView, ListCell, ProgressIndicator}
import scalafx.geometry.{Insets, Pos}
import scalafx.application.Platform
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

class RunningSimulationsView(
    apiClient: SimulationApiClient,
    onNewSimulation: () => Unit,
    onSimulationSelected: String => Unit
) extends VBox {

  spacing = 20
  padding = Insets(30)
  alignment = Pos.TopCenter
  styleClass += "running-simulations-container"

  private val title = new Label("Active Simulations") {
    styleClass += "title-label"
  }

  private val newSimButton = new Button("Start New Simulation") {
    styleClass += "primary-button"
    onAction = _ => onNewSimulation()
  }
  
  private val refreshButton = new Button("Refresh List") {
    onAction = _ => fetchSimulations()
  }
  
  private val topBar = new HBox(20) {
    alignment = Pos.Center
    children = Seq(newSimButton, refreshButton)
  }

  private val loadingIndicator = new ProgressIndicator() {
    visible = false
  }

  private val listView = new ListView[SimulationItem]() {
    vgrow = Priority.Always
    cellFactory = { _ =>
      new ListCell[SimulationItem] {
        item.onChange { (_, _, sim) =>
          if (sim != null) {
            val idLabel = new Label(s"ID: ${sim.id}")
            val presetLabel = new Label(s"Preset: ${sim.preset}")
            
            val watchButton = new Button("Watch") {
              styleClass += "secondary-button"
              onAction = _ => onSimulationSelected(sim.id)
            }
            
            val stopButton = new Button("Stop") {
              styleClass += "danger-button"
              onAction = _ => {
                apiClient.stopSimulation(sim.id).onComplete {
                  case Success(_) => Platform.runLater(fetchSimulations())
                  case Failure(e) => Platform.runLater { statusLabel.text = s"Failed to stop: ${e.getMessage}" }
                }
              }
            }
            
            val content = new HBox(20) {
              alignment = Pos.CenterLeft
              children = Seq(
                new VBox(5) {
                  children = Seq(presetLabel, idLabel)
                  hgrow = Priority.Always
                },
                watchButton,
                stopButton
              )
            }
            graphic = content
          } else {
            graphic = null
          }
        }
      }
    }
  }

  private val statusLabel = new Label("")

  children = Seq(
    title,
    topBar,
    loadingIndicator,
    listView,
    statusLabel
  )

  private def fetchSimulations(): Unit = {
    loadingIndicator.visible = true
    statusLabel.text = "Fetching simulations..."
    
    apiClient.getSimulations().onComplete {
      case Success(sims) =>
        Platform.runLater {
          loadingIndicator.visible = false
          listView.items.value.clear()
          listView.items.value.addAll(sims: _*)
          statusLabel.text = s"Loaded ${sims.size} simulations."
        }
      case Failure(e) =>
        Platform.runLater {
          loadingIndicator.visible = false
          statusLabel.text = s"Connection error: ${e.getMessage}"
        }
    }
  }

  // Fetch immediately on view load
  fetchSimulations()
}

