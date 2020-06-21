package me.msoucy.gbat

import java.io.File
import kotlin.io.forEachLine
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

fun <T> Iterable<T>.copyOf(): List<T> {
    val original = this
    return mutableListOf<T>().apply { addAll(original) }
}

fun <T> Iterable<T>.mutableCopyOf(): MutableList<T> {
    val original = this
    return mutableListOf<T>().apply { addAll(original) }
}

enum class ChangeType {
    Add, Change, Remove
}

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

    fun applyChange(changeType : ChangeType, lineNum : Int, lineText : String) = when(changeType) {
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


class KnowledgeModel(val constant : Double, val riskModel : RiskModel) {
    class KnowledgeAcct(var knowledgeAcctId : Int,
                        var authors : List<String>,
                        var authorsStr : String)

    object AuthorsTable : Table("authors") {
        val id = integer("id")
        val author = text("author")
        val idx = integer("idx").uniqueIndex()
        override val primaryKey = PrimaryKey(id)
    }
    object KnowledgeAcctsTable : Table("knowledgeaccts") {
        val id = integer("id")
        val authors = text("authors")
        val idx = integer("idx").uniqueIndex()
        override val primaryKey = PrimaryKey(id)
    }
    object KnowledgeAuthorsTable : Table("knowedgeaccts_authors") {
        val knowledgeacctid = integer("knowledgeacctid")
        val authorid = integer("authorid")
        override val primaryKey = PrimaryKey(knowledgeacctid, authorid)
    }
    object LineKnowledge : Table("lineknowledge") {
        val linenum = integer("linenum")
        val knowledgeacctid = integer("knowledgeacctid")
        val knowledge = double("knowledge")
    }

    val SAFE_AUTHOR_ID = 1
    val SAFE_KNOWLEDGE_ACCT_ID = 1
    val KNOWLEDGE_PER_LINE_ADDED = 1000.0

    fun applyChange(changeType : ChangeType, author : String, lineNum : Int) = when(changeType) {
        ChangeType.Add -> lineAdded(author, lineNum)
        ChangeType.Change -> lineChanged(author, lineNum)
        ChangeType.Remove -> lineRemoved(lineNum)
    }

    fun lineChanged(author : String, lineNum : Int) {
        val kCreated = constant * KNOWLEDGE_PER_LINE_ADDED
        val kAcquired = (1 - constant) * KNOWLEDGE_PER_LINE_ADDED
        val totLineK = totalLineKnowledge(lineNum)
        val acquiredPct = if (totLineK != 0.0) {
            kAcquired / totLineK
        } else 0.0
        redistributeKnowledge(author, lineNum, acquiredPct)
        val knowledgeAcctId = lookupOrCreateKnowledgeAcct(listOf(author))
        adjustKnowledge(knowledgeAcctId, lineNum, kCreated)
    }

    fun lineRemoved(lineNum : Int) {
        allAcctsWithKnowledgeOf(lineNum).forEach {
            destroyLineKnowledge(it, lineNum)
        }
        bumpAllLinesFrom(lineNum, -1)
    }

    fun lineAdded(author : String, lineNum : Int) {
        val knowledgeAcctId = lookupOrCreateKnowledgeAcct(listOf(author))
        bumpAllLinesFrom(lineNum-1, 1)
        adjustKnowledge(knowledgeAcctId, lineNum, KNOWLEDGE_PER_LINE_ADDED)
    }

    fun knowledgeSummary(lineNum : Int) = transaction {
        LineKnowledge.select {
            LineKnowledge.linenum eq lineNum
        }.map {
            Pair(getKnowledgeAcct(it[LineKnowledge.knowledgeacctid]).authors,
                 it[LineKnowledge.knowledge])
        }.sortedBy {
            it.first.joinToString("\n")
        }.copyOf()
    }

    private fun bumpAllLinesFrom(lineNum : Int, adjustment : Int) = transaction {
        LineKnowledge.update({LineKnowledge.linenum greater lineNum}) {
            with(SqlExpressionBuilder) {
                it[LineKnowledge.linenum] = LineKnowledge.linenum + adjustment
            }
        }
    }

    private fun getKnowledgeAcct(knowledgeAcctId : Int) = transaction {
        KnowledgeAcctsTable.select {
            KnowledgeAcctsTable.id eq knowledgeAcctId
        }.map {
            KnowledgeAcct(
                it[KnowledgeAcctsTable.id],
                it[KnowledgeAcctsTable.authors].split("\n"),
                it[KnowledgeAcctsTable.authors]
            )
        }.first()
    }

    private fun destroyLineKnowledge(knowledgeId : Int, lineNum : Int) = transaction {
        LineKnowledge.deleteWhere {
            (LineKnowledge.knowledgeacctid eq knowledgeId) and
            (LineKnowledge.linenum eq lineNum)
        }
    }

    private fun redistributeKnowledge(author : String, lineNum : Int, redistPct : Double) {
        if(riskModel.isDeparted(author)) {
            return
        }
        val knowledgeIds = nonSafeAcctsWithKnowledgeOf(lineNum)
        for (knowledgeId in knowledgeIds) {
            val knowledgeAcct = getKnowledgeAcct(knowledgeId)
            if (author !in knowledgeAcct.authors) {
                val oldKnowledge = knowledgeInAcct(knowledgeAcct.knowledgeAcctId, lineNum)
                var newAuthors = knowledgeAcct.authors.mutableCopyOf()
                if(newAuthors.all(riskModel::isDeparted)) {
                    newAuthors = mutableListOf(author)
                } else {
                    newAuthors.add(author)
                }
                newAuthors = newAuthors.sorted().mutableCopyOf()
                val newKnowledgeId = if(riskModel.jointBusProbBelowThreshold(*newAuthors.toTypedArray())) {
                    SAFE_KNOWLEDGE_ACCT_ID
                } else {
                    lookupOrCreateKnowledgeAcct(newAuthors)
                }
                val knowledgeToDist = oldKnowledge * redistPct
                adjustKnowledge(knowledgeId, lineNum, -knowledgeToDist)
                adjustKnowledge(newKnowledgeId, lineNum, knowledgeToDist)
            }
        }
    }

    private fun knowledgeInAcct(knowledgeAcctId : Int, lineNum : Int) = transaction {
        LineKnowledge.select {
            (LineKnowledge.knowledgeacctid eq knowledgeAcctId) and
            (LineKnowledge.linenum eq lineNum)
        }.map {
            it[LineKnowledge.knowledge]
        }.first()
    }

    private fun nonSafeAcctsWithKnowledgeOf(lineNum : Int) = transaction {
        LineKnowledge.select {
            (LineKnowledge.linenum eq lineNum) and
            (LineKnowledge.knowledgeacctid neq SAFE_KNOWLEDGE_ACCT_ID)
        }.map {
            it[LineKnowledge.knowledgeacctid]
        }
    }

    private fun allAcctsWithKnowledgeOf(lineNum : Int) = transaction {
        LineKnowledge.select {
            LineKnowledge.linenum eq lineNum
        }.map {
            it[LineKnowledge.knowledgeacctid]
        }
    }

    private fun adjustKnowledge(knowledgeAcctId : Int, lineNum : Int, adjustment : Double) = transaction {
        val lineExists = LineKnowledge.select {
            (LineKnowledge.knowledgeacctid eq knowledgeAcctId) and
            (LineKnowledge.linenum eq lineNum)
        }.count() > 0
        if(!lineExists) {
            LineKnowledge.insert {
                it[LineKnowledge.knowledgeacctid] = knowledgeAcctId
                it[LineKnowledge.linenum] = lineNum
                it[LineKnowledge.knowledge] = 0.0
            }
        }
        LineKnowledge.update({
            (LineKnowledge.knowledgeacctid eq knowledgeAcctId) and
            (LineKnowledge.linenum eq lineNum)
        }) { 
            with(SqlExpressionBuilder) {
                it[LineKnowledge.knowledge] = LineKnowledge.knowledge + adjustment
            }
        }
    }

    private fun lookupOrCreateKnowledgeAcct(authors : List<String>) = transaction {
        val authorStr = authors.sorted().joinToString("\n")
        var newId = -1
        KnowledgeAcctsTable.select {
            KnowledgeAcctsTable.authors eq authorStr
        }.fetchSize(1).
        forEach {
            newId = it[KnowledgeAcctsTable.id]
        }
        if (newId != -1) {
            KnowledgeAcctsTable.insert { 
                it[KnowledgeAcctsTable.authors] = authorStr
            }
            newId = KnowledgeAcctsTable.select {
                KnowledgeAcctsTable.authors eq authorStr
            }.map {
                it[KnowledgeAcctsTable.id]
            }.first()

            authors.map(::lookupOrCreateAuthor).
            forEach { authorId ->
                KnowledgeAuthorsTable.insert {
                    it[KnowledgeAuthorsTable.knowledgeacctid] = newId
                    it[KnowledgeAuthorsTable.authorid] = authorId
                }
            }
        }
        newId
    }

    private fun lookupOrCreateAuthor(authorName : String) = transaction {
        AuthorsTable.insertIgnore { 
            it[author] = authorName
        }
        AuthorsTable.select {
            AuthorsTable.author eq authorName
        }.fetchSize(1).
        map {
            it[AuthorsTable.id]
        }.first()
    }
    
    private fun totalLineKnowledge(linenum : Int) = transaction {
        LineKnowledge.select {
            LineKnowledge.linenum eq linenum
        }.fetchSize(1).
        map { it[LineKnowledge.knowledge] }.
        sum()
    }

    private fun createTables() = transaction {
        SchemaUtils.createMissingTablesAndColumns(AuthorsTable, KnowledgeAcctsTable, KnowledgeAuthorsTable, LineKnowledge)
        AuthorsTable.insertIgnore { 
            it[id] = 1
            it[author] = ""
        }
        KnowledgeAcctsTable.insertIgnore { 
            it[id] = 1
            it[authors] = ""
        }
        KnowledgeAuthorsTable.insertIgnore { 
            it[knowledgeacctid] = SAFE_KNOWLEDGE_ACCT_ID
            it[authorid] = SAFE_AUTHOR_ID
        }
    }
}