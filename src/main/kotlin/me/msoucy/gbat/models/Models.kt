package me.msoucy.gbat.models

enum class ChangeType {
    Add, Change, Remove
}

data class Event(
    val eventType : ChangeType,
    val lineNum : Int,
    val lineVal : String?
)

data class Condensation(
    val authors : List<String>,
    val knowledge : Double,
    val orphaned : Double,
    val risk : Double = 0.0
) : Comparable<Condensation> {
    override operator fun compareTo(other : Condensation) : Int {
        var result = authors.size.compareTo(other.authors.size)
        if(result == 0) {
            authors.zip(other.authors).forEach { (a, b) ->
                if(result == 0) result = a.compareTo(b)
            }
        }
        if(result == 0)
            result = knowledge.compareTo(other.knowledge)
        if(result == 0)
            result = orphaned.compareTo(other.orphaned)
        if(result == 0)
            result = risk.compareTo(other.risk)
        return result
    }
}

class CondensedAnalysis(
    var repoRoot : String = "",
    var projectRoot : String = "",
    var fileName : String = "",
    var lineSummaries : MutableList<Pair<String, List<Condensation>>> = mutableListOf()
)