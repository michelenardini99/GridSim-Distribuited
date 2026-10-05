plugins {
    scala
    `java-library`
    alias(libs.plugins.protobuf)
}

repositories {
    mavenCentral()
}

dependencies {
    api(libs.scala.library)
    api(libs.scalapb.runtime)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

val protocVersion = libs.versions.protoc.get()
val scalapbVersion = libs.versions.scalapb.get()
val isWindows = System.getProperty("os.name").lowercase().contains("win")
val scalapbArtifact = if (isWindows) {
    "com.thesamet.scalapb:protoc-gen-scala:$scalapbVersion:windows@bat"
} else {
    "com.thesamet.scalapb:protoc-gen-scala:$scalapbVersion:unix@sh"
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protocVersion"
    }
    plugins {
        create("scala") {
            artifact = scalapbArtifact
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.builtins {
                findByName("java")?.let { remove(it) }
            }
            task.plugins {
                create("scala")
            }
        }
    }
}

sourceSets {
    named("main") {
        scala {
            srcDir(layout.buildDirectory.dir("generated/source/proto/main/scala"))
        }
    }
}
