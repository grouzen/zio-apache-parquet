// Linting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.2")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.7")

// Dependencies management
addSbtPlugin("ch.epfl.scala"  % "sbt-missinglink"            % "0.3.8")
addSbtPlugin("me.mnedokushev" % "sbt2-explicit-dependencies" % "0.2.0")

// Versioning and release
addSbtPlugin("com.eed3si9n"   % "sbt-buildinfo"      % "0.13.1")
addSbtPlugin("org.typelevel"  % "sbt-tpolecat"       % "0.5.7")
addSbtPlugin("com.github.sbt" % "sbt-ci-release"     % "1.12.1")
addSbtPlugin("com.github.sbt" % "sbt-github-actions" % "0.31.0")
