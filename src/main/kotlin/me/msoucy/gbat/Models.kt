package me.msoucy.gbat

import java.io.File
import java.nio.file.Paths
import kotlin.io.forEachLine
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

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


class KnowledgeModel(val db : Database, val constant : Double, val riskModel : RiskModel) {

    class KnowledgeAcct(var knowledgeAcctId : Int,
                        var authors : List<String>,
                        var authorsStr : String)

    object AuthorsTable : Table("authors") {
        val id = integer("authorid")
        val author = text("author").uniqueIndex("authors_idx")
        override val primaryKey = PrimaryKey(id)
    }
    object KnowledgeAcctsTable : Table("knowledgeaccts") {
        val id = integer("knowledgeacctid")
        val authors = text("authors").uniqueIndex("knowledgeacctsauthors_idx")
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

    init {
        createTables()
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

    fun knowledgeSummary(lineNum : Int) = transaction(db) {
        LineKnowledge.select {
            LineKnowledge.linenum eq lineNum
        }.map {
            Pair(getKnowledgeAcct(it[LineKnowledge.knowledgeacctid]).authors,
                 it[LineKnowledge.knowledge])
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

    private fun getKnowledgeAcct(knowledgeAcctId : Int) = transaction(db) {
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

            authors.map(::lookupOrCreateAuthor).forEach { authorId ->
                KnowledgeAuthorsTable.insert {
                    it[KnowledgeAuthorsTable.knowledgeacctid] = newId
                    it[KnowledgeAuthorsTable.authorid] = authorId
                }
            }
        }
        newId
    }

    private fun lookupOrCreateAuthor(authorName : String) = transaction(db) {
        AuthorsTable.insertIgnore { 
            it[author] = authorName
        }
        AuthorsTable.select {
            AuthorsTable.author eq authorName
        }.fetchSize(1).map {
            it[AuthorsTable.id]
        }.first()
    }
    
    private fun totalLineKnowledge(linenum : Int) = transaction(db) {
        LineKnowledge.select {
            LineKnowledge.linenum eq linenum
        }.fetchSize(1).map {
            it[LineKnowledge.knowledge]
        }.first()
    }

    private fun createTables() = transaction(db) {
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

class SummaryModel(val db : Database) {

    object ProjectTable : IntIdTable("projects", "projectid") {
        val project = text("project").uniqueIndex("project_idx")
    }
    object DirsTable : IntIdTable("dirs", "dirid") {
        val dir = text("dir")
        val parentdirid = integer("parentdirid")
        val projectid = integer("projectid")
        val dirsproj_idx = uniqueIndex("dirsproj_idx", dir, parentdirid, projectid)
    }
    object FilesTable : IntIdTable("files", "fileid") {
        val fname = text("fname")
        val dirid = integer("dirid").index("filesdir_idx")
    }
    object LinesTable : IntIdTable("lines", "lineid") {
        val line = text("line")
        val fileid = integer("fileid").index("linesfile_idx")
        val linenum = integer("linenum")
        val linesnumfile_idx = uniqueIndex("linesnumfile_idx", fileid, linenum)
    }
    object AuthorsTable : IntIdTable("authors", "authorid") {
        val author = text("author").uniqueIndex("authorstrs_idx")
    }
    object AuthorsGroupsTable : IntIdTable("authorgroups", "authorgroupid") {
        val authors = text("authorsstr").uniqueIndex("authorgroupsstrs_idx")
    }
    object AuthorsAuthorGroupsTable : Table("authors_authorgroups") {
        val authorid = integer("authorid")
        val groupid = integer("authorgroupid")
        override val primaryKey = PrimaryKey(authorid, groupid)
    }
    object AllocationsTable : IntIdTable("allocations", "allocationid") {
        val knowledge = double("knowledge")
        val risk = double("risk")
        val orphaned = double("orphaned")
        val lineid = integer("lineid").index("linealloc_idx")
        val authorgroupid = integer("authorgroupid")
    }

    val GIT_BY_A_BUS_BELOW_THRESHOLD = "Git by a Bus Safe Author"

    init {
        createTables()
    }

    data class Statistics(var totKnowledge : Double = 0.0,
                          var totRisk : Double = 0.0,
                          var totOrphaned : Double = 0.0) {
        constructor(row : ResultRow) :
            this(row[AllocationsTable.knowledge.sum()] ?: 0.0,
                 row[AllocationsTable.risk.sum()] ?: 0.0,
                 row[AllocationsTable.orphaned.sum()] ?: 0.0) {}
    }
    data class AuthorRisk(var stats : Statistics = Statistics())
    data class LineDict(var stats : Statistics = Statistics(), var authorRisks : MutableMap<String, AuthorRisk> = mutableMapOf())
    class FileTree(var name : String = "",
                   var stats : Statistics = Statistics(),
                   var authorRisks : MutableMap<String, AuthorRisk> = mutableMapOf(),
                   var lines : MutableList<LineDict> = mutableListOf())

    fun fileSummary(fileId : Int) = transaction(db) {
        var fileTree = FileTree()
        val joinA = Join(LinesTable, AllocationsTable,
            JoinType.LEFT,
            LinesTable.id, AllocationsTable.lineid)
        val joinB = Join(joinA, AuthorsGroupsTable,
            JoinType.LEFT,
            AuthorsGroupsTable.id, AllocationsTable.authorgroupid
        )
        joinB.select {
            LinesTable.fileid eq fileId
        }.groupBy(AuthorsGroupsTable.id).forEach { row ->
            val authors = row[AuthorsGroupsTable.authors]
            fileTree.authorRisks[authors] =
                AuthorRisk(Statistics(row))
        }
        Join(joinA, FilesTable,
            JoinType.LEFT,
            LinesTable.fileid, FilesTable.id
        ).select {
            LinesTable.fileid eq fileId
        }.fetchSize(1).forEach { row ->
            fileTree.stats = Statistics(row)
        }

        fileTree.name = FilesTable.select {
            FilesTable.id eq fileId
        }.map { it[FilesTable.fname] }.first()

        LinesTable.select {
            LinesTable.fileid eq fileId
        }.map {
            it[LinesTable.id].value
        }.forEach { lineId ->
            val lineDict = LineDict()
            joinB.select {
                LinesTable.id eq lineId
            }.groupBy(AuthorsGroupsTable.id).forEach { lineRow ->
                lineDict.authorRisks[lineRow[AuthorsGroupsTable.authors]] = AuthorRisk(Statistics(lineRow))
            }

            joinA.select {
                LinesTable.id eq lineId
            }.fetchSize(1).forEach {
                lineDict.stats = Statistics(it)
            }
            fileTree.lines.add(lineDict)
        }
        fileTree
    }

    fun fileLines(fileId : Int) : List<String> = transaction(db) {
        LinesTable.select {
            LinesTable.fileid eq fileId
        }.orderBy(LinesTable.linenum).map {
            it[LinesTable.line]
        }
    }

    private fun Double?.zeroIfNone() = this ?: 0.0

    private fun reconsDir(dirId : Int) = transaction(db) {
        val segs = mutableListOf<String>()
        var newDirId = dirId
        while(newDirId != 0) {
            DirsTable.select {
                DirsTable.id eq newDirId
            }.forEach {
                segs.add(it[DirsTable.dir])
                newDirId = it[DirsTable.parentdirid]
            }
        }
        Paths.get(segs.reversed().joinToString("/")).normalize()
    }

    private fun safeAuthorName(author : String?) = author ?: GIT_BY_A_BUS_BELOW_THRESHOLD

    private fun createAllocation(knowledge : Double, risk : Double, orphaned : Double, authorGroupId : Int, lineId : Int) = transaction(db) {
        AllocationsTable.insert {
            it[AllocationsTable.knowledge] = knowledge
            it[AllocationsTable.risk] = risk
            it[AllocationsTable.orphaned] = orphaned
            it[AllocationsTable.lineid] = lineId
            it[AllocationsTable.authorgroupid] = authorGroupId
        }
    }

    private fun findOrCreateAuthorGroup(authors : List<String>) : Int = transaction(db) {
        val authorsstr = authors.joinToString("\n")
        var authorGroupId = AuthorsGroupsTable.select {
            AuthorsGroupsTable.authors eq authorsstr
        }.map {
            it[AuthorsGroupsTable.id].value
        }.firstOrNull()

        if (authorGroupId == null) {
            authorGroupId = AuthorsGroupsTable.insertAndGetId {
                it[AuthorsGroupsTable.authors] = authorsstr
            }.value
            authors.forEach {
                val authorId = findOrCreateAuthor(it)
                AuthorsAuthorGroupsTable.insert {
                    it[AuthorsAuthorGroupsTable.authorid] = authorId
                    it[AuthorsAuthorGroupsTable.groupid] = authorGroupId
                }
            }
        }
        authorGroupId
    }

    private fun findOrCreateAuthor(author : String) : Int = transaction(db) {
        AuthorsTable.insertIgnore {
            it[AuthorsTable.author] = author
        }
        ProjectTable.select {
            AuthorsTable.author eq author
        }.map {
            it[AuthorsTable.id].value
        }.first()
    }

    private fun createLine(line : String, lineNum : Int, fileId : Int) = transaction(db) {
        LinesTable.insertAndGetId {
            it[LinesTable.line] = line
            it[LinesTable.linenum] = lineNum
            it[LinesTable.fileid] = fileId
        }.value
    }

    private fun createFile(fname : String, parentDirId : Int) = transaction(db) {
        FilesTable.insertAndGetId {
            it[FilesTable.fname] = fname
            it[FilesTable.dirid] = parentDirId
        }.value
    }

    private fun findOrCreateProject(project : String) : Int = transaction(db) {
        ProjectTable.insertIgnore {
            it[ProjectTable.project] = project
        }
        ProjectTable.select {
            ProjectTable.project eq project
        }.map {
            it[ProjectTable.id].value
        }.first()
    }

    private fun findOrCreateDir(dirname : String, projectId : Int, parentDirId : Int) : Int = transaction(db) {
        DirsTable.insertIgnoreAndGetId {
            it[dir] = dirname
            it[parentdirid] = parentDirId
            it[projectid] = projectId
        }
        DirsTable.select {
            DirsTable.dir eq dirname
            DirsTable.parentdirid eq parentDirId
            DirsTable.projectid eq projectId
        }.map {
            it[DirsTable.id]
        }.first().value
    }

    private fun splitAllDirs(dirname : File) = dirname.toPath().iterator().asSequence().toList()

    private fun adjustFname(repoRoot : File, projectRoot : File, fname : File) : File {
        var rootDiff = projectRoot.relativeTo(repoRoot)
        return if(rootDiff.toString().length != 0) {
            fname.relativeTo(rootDiff)
        } else {
            fname
        }
    }

    private fun reconsDirs(dirId : Int) = transaction(db) {
        val dirs = mutableListOf<String>()
        var parentDirId = dirId
        while(parentDirId != 0) {
            DirsTable.select {
                DirsTable.id eq parentDirId
            }.fetchSize(1).
            forEach {
                dirs.add(it[DirsTable.dir])
                parentDirId = it[DirsTable.parentdirid]
            }
        }
        dirs.reversed()
    }

    private fun createTables() = transaction(db) {
        SchemaUtils.createMissingTablesAndColumns(
            ProjectTable,
            FilesTable,
            LinesTable,
            AuthorsTable,
            AuthorsGroupsTable,
            AuthorsAuthorGroupsTable,
            AllocationsTable
        )
    }
}