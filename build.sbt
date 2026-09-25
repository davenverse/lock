ThisBuild / tlBaseVersion := "0.1" // current series x.y

ThisBuild / organization := "io.chrisdavenport"
ThisBuild / organizationName := "Christopher Davenport"
ThisBuild / startYear := Some(2021)
ThisBuild / licenses := Seq(License.MIT)
ThisBuild / developers := List(
  tlGitHubDev("christopherdavenport", "Christopher Davenport")
)

// sbt-davenverse published a snapshot from main on every push. Dropped: the
// Central Portal will not enable snapshots for the io.chrisdavenport namespace.
ThisBuild / tlCiReleaseBranches := Seq()

val Scala213tl = "2.13.18"
ThisBuild / crossScalaVersions := Seq("2.12.20",  Scala213tl, "3.3.8")
ThisBuild / scalaVersion := Scala213tl

// Compiler settings DavenversePlugin injected globally. sbt-typelevel-ci-release
// does not supply these (only sbt-typelevel-settings would). Scoped to ThisBuild
// so every project picks them up without editing each one.
ThisBuild / libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
  case Some((2, _)) =>
    Seq(
      compilerPlugin("org.typelevel" % "kind-projector" % "0.13.4" cross CrossVersion.full),
      compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
    )
  case _ => Nil
})
ThisBuild / scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
  case Some((3, _)) => Seq("-Ykind-projector")
  case Some((2, 12)) => Seq("-Ypartial-unification")
  case _ => Nil
})


val Scala213 = "2.13.6"


ThisBuild / githubWorkflowBuild := Seq(WorkflowStep.Sbt(List("clean", "test", "mimaReportBinaryIssues")))

val catsV = "2.6.1"
val catsEffectV = "3.2.8"
val munitCatsEffectV = "1.0.5"

ThisBuild / testFrameworks += new TestFramework("munit.Framework")

// Projects
lazy val `lock` = project.in(file("."))
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
    .enablePlugins(NoPublishPlugin)
  .settings(
    name := "lock-examples"
  )

lazy val site = project.in(file("site"))
    .enablePlugins(TypelevelSitePlugin)
  .settings(
    laikaTheme := tlSiteHelium.value.site
      .topNavigationBar(
        homeLink = laika.helium.config.IconLink.internal(laika.ast.Path.Root / "index.md", laika.helium.config.HeliumIcon.home)
      )
      .build
  )
  .dependsOn(core.jvm)
  .settings{
    Seq(
    )
  }
