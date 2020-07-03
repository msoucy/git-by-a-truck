package me.msoucy.gbat

import java.io.File

import me.msoucy.gbat.models.ProjectTreeNode
import me.msoucy.gbat.models.ProjectTreeResult
import me.msoucy.gbat.models.Statistics
import me.msoucy.gbat.models.SummaryModel

import com.google.gson.GsonBuilder
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

val NUM_RISKIEST_AUTHORS = 10
val NUM_RISKIEST_FILES = 10

class SummaryRenderer(
    val summaryModel : SummaryModel,
    val outputDir : File
) {
    private val filesDir = File(outputDir, "files")
    private val gson = GsonBuilder().setPrettyPrinting().create()

    fun renderAll(projectRoot : File) {
        createFilesDir()
        renderSummaryJson(projectRoot)
        renderFileJson(projectRoot)
        // renderSrc(projectRoot)
    }

    private fun renderSummaryJson(projectRoot : File) {
        val summary = summaryModel.projectSummary(projectRoot.absolutePath)
        val json = gson.toJson(summary)
        File(filesDir, "summary.json").writeText(json)
    }

    private fun renderFileJson(projectRoot : File) {
        summaryModel.projectFiles(projectRoot.absolutePath).forEach {
            val json = gson.toJson(summaryModel.fileSummary(it.fileId))
            File(filesDir, "${it.fileId}.json").writeText(json)
        }
    }

    private fun createFilesDir() = filesDir.mkdirs()
}

fun renderSummary(
    projectRoot : File,
    summaryModel : SummaryModel,
    outputDir : File
) {
    transaction(summaryModel.db) {
        val renderer = SummaryRenderer(summaryModel, outputDir)
        renderer.renderAll(projectRoot)
    }
}