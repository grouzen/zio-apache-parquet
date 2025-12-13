// Linting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.6")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.4")

// Dependencies management
addSbtPlugin("ch.epfl.scala"    % "sbt-missinglink"           % "0.3.6")
addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.3.1")

// Versioning and release
addSbtPlugin("com.eed3si9n"   % "sbt-buildinfo"      % "0.13.1")
addSbtPlugin("org.typelevel"  % "sbt-tpolecat"       % "0.5.2")
addSbtPlugin("com.github.sbt" % "sbt-ci-release"     % "1.11.2")
addSbtPlugin("com.github.sbt" % "sbt-github-actions" % "0.29.0")

addDependencyTreePlugin
