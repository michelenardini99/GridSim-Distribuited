plugins {
    id("com.dorongold.task-tree") version "4.0.1"
}

tasks.register("integrationTest") {
    group = "verification"
    description = "Runs integration tests across all subprojects."
    dependsOn(":app:integrationTest")
}

