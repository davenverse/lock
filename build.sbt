import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}

val Scala213 = "2.13.6"

ThisBuild / crossScalaVersions := Seq("2.12.14", Scala213, "3.0.2")
ThisBuild / scalaVersion := Scala213


val catsV = "2.6.1"
val catsEffectV = "3.2.8"
val munitCatsEffectV = "1.0.5"

ThisBuild / testFrameworks += new TestFramework("munit.Framework")

// Projects
lazy val `lock` = project.in(file("."))
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .aggregate(core.jvm, core.js, examples)

  lazy val core = crossProject(JVMPlatform, JSPlatform)
    .crossType(CrossType.Pure)
    .in(file("core"))
    .settings(
      name := "lock",
      libraryDependencies ++= Seq(
        "org.typelevel"               %%% "cats-core"                  % catsV,
        "org.typelevel"               %%% "cats-effect"                % catsEffectV,
        "org.typelevel"               %%% "munit-cats-effect-3"        % munitCatsEffectV         % Test,
      )
    ).jsSettings(
      scalaJSLinkerConfig ~= { _.withModuleKind(ModuleKind.CommonJSModule)},
    )

lazy val examples = project.in(file("examples"))
  .dependsOn(core.jvm)
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .settings(
    name := "lock-examples"
  )

lazy val site = project.in(file("site"))
  .disablePlugins(MimaPlugin)
  .enablePlugins(DavenverseMicrositePlugin)
  .dependsOn(core.jvm)
  .settings{
    import microsites._
    Seq(
      micrositeDescription := "Locks for Cats-Effect",
    )
  }
