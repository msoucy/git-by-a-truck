package me.msoucy.gbat.models

import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

private object ProjectTable : IntIdTable("projects", "projectid") {
    val project = text("project").uniqueIndex("project_idx")
}
private object DirsTable : IntIdTable("dirs", "dirid") {
    val dir = text("dir")
    val parentdirid = integer("parentdirid").references(DirsTable.id)
    val projectid = integer("projectid").references(ProjectTable.id)
    val dirsproj_idx = uniqueIndex("dirsproj_idx", dir, parentdirid, projectid)
}
private object FilesTable : IntIdTable("files", "fileid") {
    val fname = text("fname")
    val dirid = integer("dirid").index("filesdir_idx").references(DirsTable.id)
}
private object LinesTable : IntIdTable("lines", "lineid") {
    val line = text("line")
    val fileid = integer("fileid").index("linesfile_idx").references(FilesTable.id)
    val linenum = integer("linenum")
    val linesnumfile_idx = uniqueIndex("linesnumfile_idx", fileid, linenum)
}
private object AuthorsTable : IntIdTable("authors", "authorid") {
    val author = text("author").uniqueIndex("authorstrs_idx")
}
private object AuthorsGroupsTable : IntIdTable("authorgroups", "authorgroupid") {
    val authors = text("authorsstr").uniqueIndex("authorgroupsstrs_idx")
}
private object AuthorsAuthorGroupsTable : Table("authors_authorgroups") {
    val authorid = integer("authorid").references(AuthorsTable.id)
    val groupid = integer("authorgroupid").references(AuthorsGroupsTable.id)
    override val primaryKey = PrimaryKey(authorid, groupid)
}
private object AllocationsTable : IntIdTable("allocations", "allocationid") {
    val knowledge = double("knowledge")
    val risk = double("risk")
    val orphaned = double("orphaned")
    val lineid = integer("lineid").index("linealloc_idx").references(LinesTable.id)
    val authorgroupid = integer("authorgroupid").references(AuthorsGroupsTable.id)
}

class LineDict {
    var stats = Statistics()
    var authorRisks = mutableMapOf<String, Statistics>()
}
class FileTree {
    var name = ""
    var stats = Statistics()
    var authorRisks = mutableMapOf<String, Statistics>()
    var lines = mutableListOf<LineDict>()
}
class FileEntry(var name: String = "") {
    var stats = Statistics()
    var authorRisks = mutableMapOf<String, Statistics>()
}
class ProjectTree {
    var name = "root"
    var files = mutableMapOf<Int, FileEntry>()
    var dirs = mutableListOf<Int>()
}
class ProjectFilesResult(var fileId: Int, var fname: Path)

data class Statistics(
    var totKnowledge: Double = 0.0,
    var totRisk: Double = 0.0,
    var totOrphaned: Double = 0.0
) {
    constructor(row: ResultRow) :
        this(row[AllocationsTable.knowledge.sum()] ?: 0.0,
             row[AllocationsTable.risk.sum()] ?: 0.0,
             row[AllocationsTable.orphaned.sum()] ?: 0.0) {}
}
class ProjectTreeNode {
    var name = "root"
    var files = mutableListOf<FileEntry>()
    var dirs = mutableListOf<ProjectTreeNode>()
}
class ProjectTreeResult(var name: String, var root: ProjectTreeNode) {
    var stats = Statistics()
    var authorRisks = mutableMapOf<String, Statistics>()
}

class SummaryModel(val db: Database) {

    val GIT_BY_A_BUS_BELOW_THRESHOLD = "Git by a Bus Safe Author"

    init {
        createTables()
    }

    private val lineAllocations = (LinesTable leftJoin AllocationsTable)
    private val lineAllocationGroups = (lineAllocations leftJoin AuthorsGroupsTable)
    private val manyJoined = (lineAllocations leftJoin FilesTable leftJoin DirsTable)
    private val allJoined = (manyJoined leftJoin AuthorsGroupsTable)

    fun summarize(ca: CondensedAnalysis) {
        val fname = adjustFname(ca.repoRoot.absoluteFile,
                                ca.projectRoot.absoluteFile,
                                ca.fileName.absoluteFile)
        val projectId = findOrCreateProject(ca.projectRoot.absolutePath)

        var parentDirId = 0
        splitAllDirs(fname.parentFile).forEach {
            parentDirId = findOrCreateDir(it.toString(), projectId, parentDirId)
        }

        val fileId = createFile(fname.name, parentDirId)

        ca.lineSummaries.forEachIndexed { index, (line, allocations) ->
            val lineNum = index + 1
            val lineId = createLine(line, lineNum, fileId)
            allocations.forEach { alloc ->
                val authors = alloc.authors.map(::safeAuthorName)
                val authorGroupId = findOrCreateAuthorGroup(authors)
                createAllocation(alloc.knowledge, alloc.risk, alloc.orphaned, authorGroupId, lineId)
            }
        }
    }

    fun totalKnowledge() = transaction(db) {
        AllocationsTable.selectAll().map { it[AllocationsTable.knowledge] }.sum()
    }

    fun totalRisk() = transaction(db) {
        AllocationsTable.selectAll().map { it[AllocationsTable.risk] }.sum()
    }

    fun totalOrphaned() = transaction(db) {
        AllocationsTable.selectAll().map { it[AllocationsTable.orphaned] }.sum()
    }

    fun countFiles() = transaction(db) {
        FilesTable.selectAll().count()
    }

    fun authorgroupsWithRisk(top: Int? = null): List<Pair<String, Double>> = transaction(db) {
        var query = (AllocationsTable innerJoin AuthorsGroupsTable)
        .selectAll()
        .groupBy(AuthorsGroupsTable.authors)
        .orderBy(AllocationsTable.risk.sum() to SortOrder.DESC)
        if (top != null) {
            query = query.limit(top)
        }
        query.map {
            it[AuthorsGroupsTable.authors] to (it[AllocationsTable.risk.sum()] ?: 0.0)
        }
    }

    fun fileidsWithRisk(top: Int? = null): List<Pair<Int, Double>> = transaction(db) {
        var query = (FilesTable leftJoin LinesTable leftJoin AllocationsTable)
        .selectAll()
        .groupBy(FilesTable.id)
        .orderBy(AllocationsTable.risk.sum() to SortOrder.DESC)
        if (top != null) {
            query = query.limit(top)
        }
        query.map {
            it[FilesTable.id].value to (it[AllocationsTable.risk.sum()] ?: 0.0)
        }
    }

    fun fpath(fileId: Int): Path = transaction(db) {
        FilesTable.select {
            FilesTable.id eq fileId
        }.first().let { row ->
            val dirs = reconsDirs(row[FilesTable.dirid])
            Paths.get(dirs.joinToString("/")).normalize()
        }
    }

    fun projectFiles(project: String): List<ProjectFilesResult> = transaction(db) {
        val projectId = findOrCreateProject(project)
        (FilesTable innerJoin DirsTable).select {
            (FilesTable.dirid eq DirsTable.id) and
            (DirsTable.projectid eq projectId)
        }.map { row ->
            ProjectFilesResult(row[FilesTable.id].value, reconsDir(row[FilesTable.dirid]).resolve(row[FilesTable.fname]))
        }
    }

    fun projectSummary(project: String) = transaction(db) {
        val projectId = findOrCreateProject(project)
        val theTree = mutableMapOf<Int, ProjectTree>().withDefault { ProjectTree() }

        // First fill in the directory structure, ignoring the files
        val parentDirIds = mutableListOf(0)
        while (parentDirIds.isNotEmpty()) {
            val parentId = parentDirIds.removeAt(0)
            DirsTable.select { DirsTable.parentdirid eq parentId }
            .forEach { row ->
                val dirId = row[DirsTable.id].value
                theTree.getOrPut(parentId) { ProjectTree() }.dirs.add(dirId)
                theTree.getOrPut(dirId) { ProjectTree() }.name = row[DirsTable.dir]
                parentDirIds.add(dirId)
            }
        }

        // Then add the files
        theTree.entries.forEach { entry ->
            FilesTable.select { FilesTable.dirid eq entry.key }.forEach { row ->
                entry.value.files[row[FilesTable.id].value] = FileEntry(row[FilesTable.fname])
            }
            entry.value.files.entries.forEach { (fileId, fileEntry) ->
                lineAllocations
                .slice(AllocationsTable.knowledge.sum(), AllocationsTable.risk.sum(), AllocationsTable.orphaned.sum())
                .select { LinesTable.fileid eq fileId }
                .groupBy(LinesTable.fileid)
                .forEach { row ->
                    fileEntry.stats.totKnowledge = row[AllocationsTable.knowledge.sum()] ?: 0.0
                    fileEntry.stats.totRisk = row[AllocationsTable.risk.sum()] ?: 0.0
                    fileEntry.stats.totOrphaned = row[AllocationsTable.orphaned.sum()] ?: 0.0
                }
            }
            entry.value.files.entries.forEach { (fileId, fileEntry) ->
                lineAllocationGroups
                .slice(
                    AllocationsTable.knowledge.sum(),
                    AllocationsTable.risk.sum(),
                    AllocationsTable.orphaned.sum(),
                    AuthorsGroupsTable.authors
                ).select { LinesTable.fileid eq fileId }
                .groupBy(AllocationsTable.authorgroupid)
                .orderBy(AuthorsGroupsTable.authors)
                .forEach { row ->
                    fileEntry.authorRisks[row[AuthorsGroupsTable.authors]] = Statistics(row)
                }
            }
        }

        val transformedRoot = transformNode(theTree, 0)
        assert(transformedRoot.dirs.size == 1)
        val root = transformedRoot.dirs.first()
        val projectTree = ProjectTreeResult(project, root)

        allJoined
        .slice(
            AllocationsTable.knowledge.sum(),
            AllocationsTable.risk.sum(),
            AllocationsTable.orphaned.sum(),
            AuthorsGroupsTable.authors
        )
        .select { DirsTable.projectid eq projectId }
        .groupBy(AuthorsGroupsTable.id)
        .forEach { row ->
            projectTree.authorRisks[row[AuthorsGroupsTable.authors]] = Statistics(row)
        }

        manyJoined
        .slice(
            AllocationsTable.knowledge.sum(),
            AllocationsTable.risk.sum(),
            AllocationsTable.orphaned.sum()
        ).select {
            DirsTable.projectid eq projectId
        }.first().let { row ->
            projectTree.stats = Statistics(row)
        }

        projectTree
    }

    fun fileSummary(fileId: Int) = transaction(db) {
        var fileTree = FileTree()
        lineAllocationGroups
        .slice(AllocationsTable.knowledge.sum(), AllocationsTable.risk.sum(), AllocationsTable.orphaned.sum(), AuthorsGroupsTable.authors)
        .select {
            LinesTable.fileid eq fileId
        }.groupBy(AuthorsGroupsTable.id).forEach { row ->
            val authors = row[AuthorsGroupsTable.authors]
            fileTree.authorRisks[authors] = Statistics(row)
        }
        Join(lineAllocations, FilesTable,
            JoinType.LEFT,
            LinesTable.fileid, FilesTable.id
        )
        .slice(AllocationsTable.knowledge.sum(), AllocationsTable.risk.sum(), AllocationsTable.orphaned.sum())
        .select { LinesTable.fileid eq fileId }
        .first().let { row ->
            fileTree.stats = Statistics(row)
        }

        fileTree.name = FilesTable.select { FilesTable.id eq fileId }.map { it[FilesTable.fname] }.first()

        LinesTable.select { LinesTable.fileid eq fileId }
        .map { it[LinesTable.id].value }
        .forEach { lineId ->
            val lineDict = LineDict()
            lineAllocationGroups
            .slice(AllocationsTable.knowledge.sum(), AllocationsTable.risk.sum(), AllocationsTable.orphaned.sum(), AuthorsGroupsTable.authors)
            .select {
                LinesTable.id eq lineId
            }.groupBy(AuthorsGroupsTable.id).forEach { lineRow ->
                lineDict.authorRisks[lineRow[AuthorsGroupsTable.authors]] = Statistics(lineRow)
            }

            lineAllocations
            .slice(AllocationsTable.knowledge.sum(), AllocationsTable.risk.sum(), AllocationsTable.orphaned.sum())
            .select {
                LinesTable.id eq lineId
            }.first().let {
                lineDict.stats = Statistics(it)
            }
            fileTree.lines.add(lineDict)
        }
        fileTree
    }

    fun fileLines(fileId: Int): List<String> = transaction(db) {
        LinesTable.select {
            LinesTable.fileid eq fileId
        }.orderBy(LinesTable.linenum).map {
            it[LinesTable.line]
        }
    }

    private fun transformNode(tree: MutableMap<Int, ProjectTree>, dirId: Int): ProjectTreeNode {
        val result = ProjectTreeNode()
        tree[dirId]?.let { dirdict ->
            result.name = dirdict.name
            result.dirs = mutableListOf<ProjectTreeNode>().apply {
                dirdict.dirs.forEach {
                    add(transformNode(tree, it))
                    tree.remove(it)
                }
            }
            result.files = dirdict.files.values.toMutableList()
        }
        return result
    }

    private fun reconsDir(dirId: Int) = transaction(db) {
        val segs = mutableListOf<String>()
        var newDirId = dirId
        while (newDirId != 0) {
            DirsTable.select {
                DirsTable.id eq newDirId
            }.forEach {
                segs.add(it[DirsTable.dir])
                newDirId = it[DirsTable.parentdirid]
            }
        }
        Paths.get(segs.reversed().joinToString("/")).normalize()
    }

    private fun safeAuthorName(author: String?) = author ?: GIT_BY_A_BUS_BELOW_THRESHOLD

    private fun createAllocation(knowledge: Double, risk: Double, orphaned: Double, authorGroupId: Int, lineId: Int) = transaction(db) {
        AllocationsTable.insert {
            it[AllocationsTable.knowledge] = knowledge
            it[AllocationsTable.risk] = risk
            it[AllocationsTable.orphaned] = orphaned
            it[AllocationsTable.lineid] = lineId
            it[AllocationsTable.authorgroupid] = authorGroupId
        }
    }

    private fun findOrCreateAuthorGroup(authors: List<String>): Int = transaction(db) {
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

    private fun findOrCreateAuthor(author: String): Int = transaction(db) {
        AuthorsTable.insertIgnore {
            it[AuthorsTable.author] = author
        }
        AuthorsTable.select {
            AuthorsTable.author eq author
        }.map {
            it[AuthorsTable.id].value
        }.first()
    }

    private fun createLine(line: String, lineNum: Int, fileId: Int) = transaction(db) {
        LinesTable.insertAndGetId {
            it[LinesTable.line] = line
            it[LinesTable.linenum] = lineNum
            it[LinesTable.fileid] = fileId
        }.value
    }

    private fun createFile(fname: String, parentDirId: Int) = transaction(db) {
        FilesTable.insertAndGetId {
            it[FilesTable.fname] = fname
            it[FilesTable.dirid] = parentDirId
        }.value
    }

    private fun findOrCreateProject(project: String): Int = transaction(db) {
        ProjectTable.insertIgnore {
            it[ProjectTable.project] = project
        }
        ProjectTable.select {
            ProjectTable.project eq project
        }.map {
            it[ProjectTable.id].value
        }.first()
    }

    private fun findOrCreateDir(dirname: String, projectId: Int, parentDirId: Int): Int = transaction(db) {
        DirsTable.insertIgnore {
            it[dir] = dirname
            it[parentdirid] = parentDirId
            it[projectid] = projectId
        }
        DirsTable.select {
            DirsTable.dir eq dirname
            DirsTable.parentdirid eq parentDirId
            DirsTable.projectid eq projectId
        }.map {
            it[DirsTable.id].value
        }.first()
    }

    private fun splitAllDirs(dirname: File) = dirname.toPath().iterator().asSequence().toList()

    private fun adjustFname(repoRoot: File, projectRoot: File, fname: File): File {
        val rootDiff = if (projectRoot.canonicalPath != repoRoot.canonicalPath) {
            projectRoot.relativeTo(repoRoot)
        } else {
            repoRoot
        }
        return if (rootDiff.toString().length != 0) {
            fname.relativeTo(rootDiff)
        } else {
            fname
        }
    }

    private fun reconsDirs(dirId: Int) = transaction(db) {
        val dirs = mutableListOf<String>()
        var parentDirId = dirId
        while (parentDirId != 0) {
            DirsTable.select {
                DirsTable.id eq parentDirId
            }
            .first()
            .let {
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
