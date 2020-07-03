package me.msoucy.gbat.models

import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.forEachLine
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

class LineModel() {
    inner class Line(var num : Int, var text : String)
    val model = mutableSetOf<Line>()

    fun apply(changeType : ChangeType, lineNum : Int, lineText : String) = when(changeType) {
        ChangeType.Add -> add(Line(lineNum, lineText))
        ChangeType.Change -> change(Line(lineNum, lineText))
        ChangeType.Remove -> del(Line(lineNum, lineText))
    }

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
