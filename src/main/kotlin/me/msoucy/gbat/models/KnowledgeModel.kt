package me.msoucy.gbat.models

import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

import me.msoucy.gbat.copyOf
import me.msoucy.gbat.mutableCopyOf

class KnowledgeModel(val db : Database, val constant : Double, val riskModel : RiskModel) {

    class KnowledgeAcct(var knowledgeAcctId : Int,
                        var authors : List<String>,
                        var authorsStr : String)

    object AuthorsTable : IntIdTable("authors", "authorid") {
        val author = text("author").uniqueIndex("authors_idx")
    }
    object KnowledgeAcctsTable : IntIdTable("knowledgeaccts", "knowledgeacctid") {
        val authors = text("authors").uniqueIndex("knowledgeacctsauthors_idx")
    }
    object KnowledgeAuthorsTable : Table("knowedgeaccts_authors") {
        val knowledgeacctid = integer("knowledgeacctid")
        val authorid = integer("authorid")
        override val primaryKey = PrimaryKey(knowledgeacctid, authorid)
    }
    object LineKnowledge : Table("lineknowledge") {
        val linenum = integer("linenum")
        val knowledgeacctid = integer("knowledgeacctid").references(KnowledgeAuthorsTable.knowledgeacctid)
        val knowledge = double("knowledge")
    }

    init {
        createTables()
    }

    val SAFE_AUTHOR_ID = 1
    val SAFE_KNOWLEDGE_ACCT_ID = 1
    val KNOWLEDGE_PER_LINE_ADDED = 1000.0

    fun apply(changeType : ChangeType, author : String, lineNum : Int) = when(changeType) {
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

    fun knowledgeSummary(lineNum : Int) = transaction(db) {
        LineKnowledge.select {
            LineKnowledge.linenum eq lineNum
        }.map {
            val acct = getKnowledgeAcct(it[LineKnowledge.knowledgeacctid])
            Pair(acct.authors, it[LineKnowledge.knowledge])
        }.sortedBy {
            it.first.joinToString("\n")
        }.copyOf()
    }

    private fun bumpAllLinesFrom(lineNum : Int, adjustment : Int) = transaction(db) {
        LineKnowledge.update({LineKnowledge.linenum greater lineNum}) {
            with(SqlExpressionBuilder) {
                it[LineKnowledge.linenum] = LineKnowledge.linenum + adjustment
            }
        }
    }

    private fun getKnowledgeAcct(knowledgeAcctId : Int) : KnowledgeAcct = transaction(db) {
        KnowledgeAcctsTable.select {
            KnowledgeAcctsTable.id eq knowledgeAcctId
        }.map {
            KnowledgeAcct(
                it[KnowledgeAcctsTable.id].value,
                it[KnowledgeAcctsTable.authors].split("\n"),
                it[KnowledgeAcctsTable.authors]
            )
        }.firstOrNull() ?: KnowledgeAcct(-1, listOf(), "")
    }

    private fun destroyLineKnowledge(knowledgeId : Int, lineNum : Int) = transaction(db) {
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
                val newKnowledgeId = if(riskModel.jointBusProbBelowThreshold(newAuthors)) {
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

    private fun knowledgeInAcct(knowledgeAcctId : Int, lineNum : Int) = transaction(db) {
        LineKnowledge.select {
            (LineKnowledge.knowledgeacctid eq knowledgeAcctId) and
            (LineKnowledge.linenum eq lineNum)
        }.map {
            it[LineKnowledge.knowledge]
        }.first()
    }

    private fun nonSafeAcctsWithKnowledgeOf(lineNum : Int) = transaction(db) {
        LineKnowledge.select {
            (LineKnowledge.linenum eq lineNum) and
            (LineKnowledge.knowledgeacctid neq SAFE_KNOWLEDGE_ACCT_ID)
        }.map {
            it[LineKnowledge.knowledgeacctid]
        }
    }

    private fun allAcctsWithKnowledgeOf(lineNum : Int) = transaction(db) {
        LineKnowledge.select {
            LineKnowledge.linenum eq lineNum
        }.map {
            it[LineKnowledge.knowledgeacctid]
        }
    }

    private fun adjustKnowledge(knowledgeAcctId : Int, lineNum : Int, adjustment : Double) = transaction(db) {
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

    private fun lookupOrCreateKnowledgeAcct(authors : List<String>) = transaction(db) {
        val authorStr = authors.sorted().joinToString("\n")
        KnowledgeAcctsTable.select {
            KnowledgeAcctsTable.authors eq authorStr
        }.map {
            it[KnowledgeAcctsTable.id].value
        }.firstOrNull() ?: run {
            KnowledgeAcctsTable.insert { 
                it[KnowledgeAcctsTable.authors] = authorStr
            }
            val theNewId = KnowledgeAcctsTable.select {
                KnowledgeAcctsTable.authors eq authorStr
            }.map {
                it[KnowledgeAcctsTable.id].value
            }.first()

            authors.map(::lookupOrCreateAuthor).forEach { authorId ->
                KnowledgeAuthorsTable.insert {
                    it[KnowledgeAuthorsTable.knowledgeacctid] = theNewId
                    it[KnowledgeAuthorsTable.authorid] = authorId.value
                }
            }
            theNewId
        }
    }

    private fun lookupOrCreateAuthor(authorName : String) = transaction(db) {
        AuthorsTable.insertIgnore { 
            it[author] = authorName
        }
        AuthorsTable.select {
            AuthorsTable.author eq authorName
        }.first().let {
            it[AuthorsTable.id]
        }
    }
    
    private fun totalLineKnowledge(linenum : Int) = transaction(db) {
        LineKnowledge.select {
            LineKnowledge.linenum eq linenum
        }.map {
            it[LineKnowledge.knowledge]
        }.firstOrNull() ?: 0.0
    }

    private fun createTables() = transaction(db) {
        SchemaUtils.dropDatabase()
        SchemaUtils.createMissingTablesAndColumns(AuthorsTable, KnowledgeAcctsTable, KnowledgeAuthorsTable, LineKnowledge)
        AuthorsTable.insertIgnore {
            it[author] = ""
        }
        KnowledgeAcctsTable.insertIgnore {
            it[authors] = ""
        }
        KnowledgeAuthorsTable.insertIgnore {
            it[knowledgeacctid] = SAFE_KNOWLEDGE_ACCT_ID
            it[authorid] = SAFE_AUTHOR_ID
        }
    }
}
