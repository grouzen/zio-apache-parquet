// Linting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.4")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.2")

// Dependencies management
addSbtPlugin("ch.epfl.scala"    % "sbt-missinglink"           % "0.3.6")
addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.3.1")

// Versioning and release
addSbtPlugin("com.eed3si9n"   % "sbt-buildinfo"      % "0.13.1")
addSbtPlugin("org.typelevel"  % "sbt-tpolecat"       % "0.5.2")
addSbtPlugin("com.github.sbt" % "sbt-ci-release"     % "1.9.3")
addSbtPlugin("com.github.sbt" % "sbt-github-actions" % "0.24.0")

addDependencyTreePlugin
