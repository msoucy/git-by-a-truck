package me.msoucy.gbat.models

import java.io.File
import kotlin.io.forEachLine

class RiskModel(
    val threshold: Double,
    val default: Double,
    val busRiskFile: File?,
    val departedFile: File?
) {
    val departed = mutableSetOf<String>()
    val risks = mutableMapOf<String, Double>().withDefault { default }

    init {
        parseBusRisks()
        parseDeparted()
    }

    operator fun get(author: String): Double {
        val name = author.trim()
        if (name.isEmpty()) {
            return threshold
        }
        return risks.getOrPut(name) { default }
    }

    fun isDeparted(author: String) = author.trim() in departed

    fun jointBusProb(authors: List<String>) =
        (authors.map { this[it] } + 1.0).reduce { a, b -> a * b }

    fun jointBusProbBelowThreshold(authors: List<String>) =
        jointBusProb(authors) <= threshold

    private fun parseBusRisks() {
        busRiskFile?.forEachLine { line ->
            val sline = line.trim()
            if (sline.isNotEmpty()) {
                val segments = sline.split("=")
                val risk = segments.last()
                val author = segments.dropLast(1).joinToString(separator = "=")
                risks[author] = risk.toDouble()
            }
        }
    }

    private fun parseDeparted() {
        departedFile?.forEachLine { line ->
            val author = line.trim()
            if (author.isNotEmpty()) {
                risks[author] = 1.0
                departed.add(author)
            }
        }
    }
}
