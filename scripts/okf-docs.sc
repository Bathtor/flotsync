#!/usr/bin/env amm
import $ivy.`org.yaml:snakeyaml:2.4`

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.{Map => JMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

import org.yaml.snakeyaml.Yaml

val knownTypes = Vector(
  "API Contract",
  "Benchmark",
  "Crate Overview",
  "Decision",
  "Protocol",
  "Scenario",
  "State Machine",
  "Testing Guide",
  "Wire Format",
)

val knownStatuses = Set(
  "draft",
  "settled",
  "implemented",
  "superseded",
  "deferred",
)

val requiredFields = Vector("type", "title", "description", "status")
val relationshipFields = Vector("depends_on", "implements", "supersedes")
val linkPattern: Regex = """\[[^\]]+\]\(([^)]+)\)""".r

case class Concept(
  path: Path,
  docsRelative: String,
  frontmatter: Map[String, Any],
  body: String,
) {
  def field(name: String): Option[String] =
    frontmatter.get(name).collect { case value: String => value.trim }.filter(_.nonEmpty)

  def required(name: String): String = field(name).getOrElse("")
}

case class Diagnostic(path: Option[Path], message: String) {
  def render(root: Path): String = {
    val prefix = path match {
      case Some(value) => s"${root.relativize(value).toString}: "
      case None        => ""
    }
    s"${prefix}${message}"
  }
}

def markdownFiles(docsDir: Path): Vector[Path] = {
  if (!Files.exists(docsDir)) {
    Vector.empty
  } else {
    val stream = Files.walk(docsDir)
    try {
      stream
        .iterator()
        .asScala
        .filter(path => Files.isRegularFile(path))
        .filter(path => path.getFileName.toString.endsWith(".md"))
        .filter(path => path.getFileName.toString != "index.md")
        .toVector
        .sortBy(path => docsDir.relativize(path).toString)
    } finally {
      stream.close()
    }
  }
}

def normalisePath(path: Path): String =
  path.iterator().asScala.map(_.toString).mkString("/")

def readUtf8(path: Path): String =
  Files.readString(path, StandardCharsets.UTF_8)

def parseFrontmatter(path: Path): Either[String, (Map[String, Any], String)] = {
  val text = readUtf8(path).replace("\r\n", "\n")
  val lines = text.split("\n", -1).toVector
  if (lines.headOption.exists(_.trim == "---")) {
    val end = lines.indexWhere(_.trim == "---", 1)
    if (end < 0) {
      Left("frontmatter starts with --- but has no closing ---")
    } else {
      val yamlText = lines.slice(1, end).mkString("\n")
      val body = lines.drop(end + 1).mkString("\n")
      val yaml = new Yaml()
      try {
        val loaded = yaml.load[JMap[String, Any]](yamlText)
        val parsed =
          if (loaded == null) Map.empty[String, Any]
          else loaded.asScala.toMap
        Right((parsed, body))
      } catch {
        case error: Exception => Left(s"invalid YAML frontmatter: ${error.getMessage}")
      }
    }
  } else {
    Left("missing YAML frontmatter")
  }
}

def loadConcepts(root: Path, docsDir: Path): (Vector[Concept], Vector[Diagnostic]) = {
  val concepts = mutable.Buffer.empty[Concept]
  val diagnostics = mutable.Buffer.empty[Diagnostic]

  for (path <- markdownFiles(docsDir)) {
    parseFrontmatter(path) match {
      case Right((frontmatter, body)) =>
        concepts += Concept(
          path,
          normalisePath(docsDir.relativize(path)),
          frontmatter,
          body,
        )
      case Left(message) =>
        diagnostics += Diagnostic(Some(path), message)
    }
  }

  (concepts.toVector, diagnostics.toVector)
}

def stringList(value: Any): Vector[String] = value match {
  case text: String => Vector(text.trim).filter(_.nonEmpty)
  case values: java.lang.Iterable[_] =>
    values.asScala.toVector.flatMap {
      case text: String => Some(text.trim).filter(_.nonEmpty)
      case other        => Some(other.toString.trim).filter(_.nonEmpty)
    }
  case other => Vector(other.toString.trim).filter(_.nonEmpty)
}

def validateConceptFields(concepts: Vector[Concept]): Vector[Diagnostic] = {
  val diagnostics = mutable.Buffer.empty[Diagnostic]

  for (concept <- concepts) {
    for (field <- requiredFields) {
      concept.frontmatter.get(field) match {
        case Some(value: String) if value.trim.nonEmpty =>
        case Some(_) =>
          diagnostics += Diagnostic(Some(concept.path), s"required field '${field}' must be a non-empty string")
        case None =>
          diagnostics += Diagnostic(Some(concept.path), s"missing required field '${field}'")
      }
    }

    concept.field("type").foreach { value =>
      if (!knownTypes.contains(value)) {
        diagnostics += Diagnostic(Some(concept.path), s"unknown type '${value}'")
      }
    }

    concept.field("status").foreach { value =>
      if (!knownStatuses.contains(value)) {
        diagnostics += Diagnostic(Some(concept.path), s"unknown status '${value}'")
      }
    }
  }

  val titles = concepts.flatMap(concept => concept.field("title").map(_ -> concept.path))
  for ((title, entries) <- titles.groupBy(_._1) if entries.size > 1) {
    val paths = entries.map(_._2.getFileName.toString).sorted.mkString(", ")
    diagnostics += Diagnostic(None, s"duplicate title '${title}' in ${paths}")
  }

  diagnostics.toVector
}

def validateRelationships(concepts: Vector[Concept]): Vector[Diagnostic] = {
  val knownTargets = concepts.flatMap { concept =>
    Vector(
      concept.docsRelative,
      concept.docsRelative.stripSuffix(".md"),
      s"docs/${concept.docsRelative}",
    )
  }.toSet

  val diagnostics = mutable.Buffer.empty[Diagnostic]
  for {
    concept <- concepts
    field <- relationshipFields
    value <- concept.frontmatter.get(field).toVector
    target <- stringList(value)
  } {
    if (!knownTargets.contains(target)) {
      diagnostics += Diagnostic(Some(concept.path), s"${field} target '${target}' does not match a concept document")
    }
  }
  diagnostics.toVector
}

def stripLinkFragment(target: String): String =
  target.takeWhile(_ != '#')

def isExternalLink(target: String): Boolean =
  target.matches("^[A-Za-z][A-Za-z0-9+.-]*:.*")

def validateMarkdownLinks(root: Path, concepts: Vector[Concept]): Vector[Diagnostic] = {
  val diagnostics = mutable.Buffer.empty[Diagnostic]

  for (concept <- concepts) {
    for (linkMatch <- linkPattern.findAllMatchIn(concept.body)) {
      val rawTarget = linkMatch.group(1).trim
      val target = stripLinkFragment(rawTarget)
      if (target.nonEmpty && !isExternalLink(target)) {
        val resolved =
          if (target.startsWith("/")) {
            root.resolve(target.dropWhile(_ == '/')).normalize()
          } else {
            concept.path.getParent.resolve(target).normalize()
          }

        if (!Files.exists(resolved)) {
          diagnostics += Diagnostic(Some(concept.path), s"broken local link '${rawTarget}'")
        }
      }
    }
  }

  diagnostics.toVector
}

def indexContent(concepts: Vector[Concept]): String = {
  val builder = new StringBuilder
  builder.append("# Flotsync Design Knowledge Index\n\n")
  builder.append("This file is generated by `amm scripts/okf-docs.sc generate-index`.\n")
  builder.append("Do not edit it directly.\n")

  val byType = concepts.groupBy(_.required("type"))
  for (conceptType <- knownTypes if byType.contains(conceptType)) {
    builder.append("\n")
    builder.append(s"## ${conceptType}\n\n")
    val entries = byType(conceptType).sortBy(concept => concept.required("title").toLowerCase)
    for (concept <- entries) {
      val title = concept.required("title")
      val description = concept.required("description")
      builder.append(s"- [${title}](${concept.docsRelative}) - ${description}\n")
    }
  }

  builder.toString()
}

def validateGeneratedIndex(root: Path, docsDir: Path, concepts: Vector[Concept]): Vector[Diagnostic] = {
  val indexPath = docsDir.resolve("index.md")
  val expected = indexContent(concepts)
  if (!Files.exists(indexPath)) {
    Vector(Diagnostic(Some(indexPath), "generated index is missing"))
  } else {
    val actual = readUtf8(indexPath).replace("\r\n", "\n")
    if (actual == expected) Vector.empty
    else Vector(Diagnostic(Some(indexPath), "generated index is stale; run amm scripts/okf-docs.sc generate-index"))
  }
}

def collectDiagnostics(root: Path, docsDir: Path, concepts: Vector[Concept], parseDiagnostics: Vector[Diagnostic]): Vector[Diagnostic] =
  parseDiagnostics ++
    validateConceptFields(concepts) ++
    validateRelationships(concepts) ++
    validateMarkdownLinks(root, concepts) ++
    validateGeneratedIndex(root, docsDir, concepts)

def failWithDiagnostics(root: Path, diagnostics: Vector[Diagnostic]): Unit = {
  Console.err.println("OKF docs check failed:")
  for (diagnostic <- diagnostics) {
    Console.err.println(s"- ${diagnostic.render(root)}")
  }
  sys.exit(1)
}

@main
def main(command: String = "check"): Unit = {
  val root = Paths.get("").toAbsolutePath.normalize()
  val docsDir = root.resolve("docs")
  val (concepts, parseDiagnostics) = loadConcepts(root, docsDir)

  command match {
    case "check" =>
      val diagnostics = collectDiagnostics(root, docsDir, concepts, parseDiagnostics)
      if (diagnostics.nonEmpty) failWithDiagnostics(root, diagnostics)
      println(s"OKF docs check passed for ${concepts.size} concept documents.")

    case "generate-index" =>
      val diagnostics =
        parseDiagnostics ++ validateConceptFields(concepts) ++ validateRelationships(concepts)
      if (diagnostics.nonEmpty) failWithDiagnostics(root, diagnostics)
      Files.writeString(docsDir.resolve("index.md"), indexContent(concepts), StandardCharsets.UTF_8)
      println(s"Generated docs/index.md for ${concepts.size} concept documents.")

    case other =>
      Console.err.println(s"unknown command '${other}'")
      Console.err.println("usage: amm scripts/okf-docs.sc [check|generate-index]")
      sys.exit(2)
  }
}
