package me.msoucy.gbat

import com.google.gson.GsonBuilder
import java.io.File
import me.msoucy.gbat.models.SummaryModel
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import org.python.util.PythonInterpreter

val NUM_RISKIEST_AUTHORS = 10
val NUM_RISKIEST_FILES = 10

class SummaryRenderer(
    val summaryModel: SummaryModel,
    val outputDir: File
) {
    private val filesDir = File(outputDir, "files")
    private val gson = GsonBuilder().setPrettyPrinting().create()

    fun renderAll(projectRoot: File) {
        createFilesDir()
        renderSummaryJson(projectRoot)
        renderFileJson(projectRoot)
        renderSrc(projectRoot)
    }

    private fun renderSummaryJson(projectRoot: File) {
        val summary = summaryModel.projectSummary(projectRoot.absolutePath)
        val json = gson.toJson(summary)
        File(filesDir, "summary.json").writeText(json)
    }

    private fun renderFileJson(projectRoot: File) {
        summaryModel.projectFiles(projectRoot.absolutePath).forEach {
            val json = gson.toJson(summaryModel.fileSummary(it.fileId))
            File(filesDir, "${it.fileId}.json").writeText(json)
        }
    }

    private fun createFilesDir() = filesDir.mkdirs()

    private fun renderSrc(projectRoot: File) {
        val interpreter = PythonInterpreter()
        val cssFile = File(filesDir, "pygments.css")
        interpreter.exec("""
from pygments.formatters import HtmlFormatter
formatter = HtmlFormatter(linenos=True, lineanchors='gbab')
formatCss = formatter.get_style_defs()
""")
        cssFile.writeText(interpreter.get("formatCss", String::class.java))

        summaryModel.projectFiles(projectRoot.absolutePath).forEach {
            val resultFile = File(filesDir, "${it.fileId}.html")
            val lines = summaryModel.fileLines(it.fileId)
            val body = lines.joinToString("\n")
            interpreter["fname"] = it.fname.toString()
            interpreter["body"] = body
            interpreter.exec("""
from pygments import highlight
from pygments.lexers import guess_lexer_for_filename
lexer = guess_lexer_for_filename(fname, body)
html = highlight(body, lexer, formatter)
""")
            resultFile.writeText("""<link rel=stylesheet type="text/css" href="pygments.css">""" + interpreter.get("html", String::class.java))
        }
    }
}

fun renderSummary(
    projectRoot: File,
    summaryModel: SummaryModel,
    outputDir: File
) {
    transaction(summaryModel.db) {
        val renderer = SummaryRenderer(summaryModel, outputDir)
        renderer.renderAll(projectRoot)
    }
}
