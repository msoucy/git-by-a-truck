package me.msoucy.gbat

import java.io.File

import me.msoucy.gbat.models.KnowledgeModel
import me.msoucy.gbat.models.LineModel
import me.msoucy.gbat.models.RiskModel

private data class Condensation(val authors : List<String>, val knowledge : Double, val orphaned : Double, val atRisk : Double = 0.0) : Comparable<Condensation> {
    override operator fun compareTo(other : Condensation) : Int {
        return -1
    }
}
private class Result(val repoRoot : File,
                     val projectRoot : File,
                     val fname : File,
                     val results : List<Pair<String, List<Condensation>>>)

private fun condenseAnalysis(repoRoot : File,
                             projectRoot : File,
                             fname : File,
                             lineModel : LineModel,
                             knowledgeModel : KnowledgeModel,
                             riskModel : RiskModel) : Result {
    val condensations = lineModel.get().mapIndexed { idx, line ->
        val knowledges = knowledgeModel.knowledgeSummary(idx + 1).map { (authors, knowledge) ->
            Condensation(authors,
                         knowledge,
                         if(authors.all(riskModel::isDeparted)) knowledge else 0.0,
                         riskModel.jointBusProb(authors) * knowledge)
        }.sorted()
        Pair(line, knowledges)
    }
    return Result(repoRoot, projectRoot, fname, condensations)
}