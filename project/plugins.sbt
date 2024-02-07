// Linting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.11.1")

// Dependencies management
addSbtPlugin("ch.epfl.scala"    % "sbt-missinglink"           % "0.3.6")
addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.3.1")

// Versioning and release
addSbtPlugin("com.eed3si9n"   % "sbt-buildinfo"      % "0.11.0")
addSbtPlugin("org.typelevel"  % "sbt-tpolecat"       % "0.5.0")
addSbtPlugin("com.github.sbt" % "sbt-ci-release"     % "1.5.12")
addSbtPlugin("com.github.sbt" % "sbt-github-actions" % "0.22.0")

addDependencyTreePlugin
