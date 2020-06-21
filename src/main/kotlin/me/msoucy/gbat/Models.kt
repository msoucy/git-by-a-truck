package me.msoucy.gbat

import java.io.File
import kotlin.io.forEachLine

class RiskModel(val threshold : Double,
                val default : Double,
                val busRiskFile : File?,
                val departedFile : File?) {
    val departed = mutableSetOf<String>()
    val risks = mutableMapOf<String, Double>().withDefault {default}

    init {
        parseBusRisks()
        parseDeparted()
    }

    operator fun get(author : String) : Double {
        val name = author.trim()
        if(name.isEmpty()) {
            return threshold
        }
        return risks.getOrPut(name) { default }
    }

    fun isDeparted(author : String) = author.trim() in departed

    fun jointBusProb(vararg authors : String) =
        authors.map { this[it] }.reduce { a, b -> a * b }

    fun jointBusProbBelowThreshold(vararg authors : String) =
        jointBusProb(*authors) <= threshold
    
    private fun parseBusRisks() {
        busRiskFile?.forEachLine { line ->
            val sline = line.trim()
            if(sline.isNotEmpty()) {
                val segments = sline.split("=")
                val risk = segments.last()
                val author = segments.dropLast(1).joinToString(separator="=")
                risks[author] = risk.toDouble()
            }
        }
    }
    
    private fun parseDeparted() {
        departedFile?.forEachLine { line ->
            val author = line.trim()
            if(author.isNotEmpty()) {
                risks[author] = 1.0
                departed.add(author)
            }
        }
    }
}

class LineModel() {
    inner class Line(var num : Int, var text : String)
    val model = mutableSetOf<Line>()

    fun add(line : Line) {
        model.onEach { entry ->
            if(entry.num >= line.num) {
                entry.num++
            }
        }
        model.add(line)
    }

    fun del(line : Line) {
        model.removeIf { it.num == line.num }
        model.onEach { entry ->
            if(entry.num > line.num) {
                entry.num--
            }
        }
    }

    fun change(line : Line) {
        model.removeIf { it.num == line.num }
        model.add(line)
    }

    fun get() = model.sortedBy { it.num }.map { it.text }
}