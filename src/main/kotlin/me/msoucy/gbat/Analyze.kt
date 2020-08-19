package me.msoucy.gbat

import java.io.File
import me.msoucy.gbat.models.Condensation
import me.msoucy.gbat.models.CondensedAnalysis
import me.msoucy.gbat.models.KnowledgeModel
import me.msoucy.gbat.models.LineModel
import me.msoucy.gbat.models.RiskModel
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

fun analyze(
    riskModel: RiskModel,
    createdConstant: Double,
    historyItem: HistoryItem,
    verbose: Boolean = false
): CondensedAnalysis {
    val lineModel = LineModel()
    val db = Database.connect("jdbc:sqlite::memory:", "org.sqlite.JDBC")
    return transaction(db) {
        val knowledgeModel = KnowledgeModel(db, createdConstant, riskModel)
        var changesProcessed = 0

        historyItem.authorDiffs.forEach { (author, changes) ->
            changes.forEach { change ->
                changesProcessed++
                if (changesProcessed % 1000 == 0 && verbose) {
                    System.err.println("Analyzer applied change #$changesProcessed")
                }
                lineModel.apply(change.eventType, change.lineNum, change.lineVal ?: "")
                knowledgeModel.apply(change.eventType, author, change.lineNum)
            }
        }

        condenseAnalysis(
            historyItem.repoRoot,
            historyItem.projectRoot,
            historyItem.fname,
            lineModel,
            knowledgeModel,
            riskModel)
    }
}

private fun condenseAnalysis(
    repoRoot: File,
    projectRoot: File,
    fname: File,
    lineModel: LineModel,
    knowledgeModel: KnowledgeModel,
    riskModel: RiskModel
): CondensedAnalysis {
    val condensations = lineModel.get().mapIndexed { idx, line ->
        val knowledges = knowledgeModel.knowledgeSummary(idx + 1).map { (authors, knowledge) ->
            Condensation(authors,
                         knowledge,
                         if (authors.all(riskModel::isDeparted)) knowledge else 0.0,
                         riskModel.jointBusProb(authors) * knowledge)
        }.sorted()
        Pair(line, knowledges)
    }
    return CondensedAnalysis(repoRoot, projectRoot, fname, condensations.mutableCopyOf())
}
