package me.msoucy.gbat

import java.io.File
import kotlin.math.abs
import kotlin.math.max

import me.msoucy.gbat.models.ChangeType
import me.msoucy.gbat.models.Event

data class HistoryItem(
    val repoRoot : File,
    val projectRoot : File,
    val fname : File,
    val authorDiffs : List<Pair<String, List<Event>>>
)

fun parseHistory(repo : GitRepo,
                 projectRoot : File,
                 fname : File,
                 verbose : Boolean = false) : HistoryItem {
    val entries = repo.log(fname)
    val repoRoot = repo.root()
    if(verbose) {
        System.err.println("Parsing history for ${fname}")
    }
    return HistoryItem(repoRoot, projectRoot, fname,
        entries.map { (author, diff) ->
            Pair(author.trim(), diffWalk(diff))
        }
    )
}

fun diffWalk(diff : Diff) : List<Event> {

    fun String.startsChunk() = startsWith("@@")
    fun String.isOldLine() = startsWith("-")
    fun String.isNewLine() = startsWith("+")

    fun chunkify() : List<List<String>> {
        val chunks = mutableListOf<MutableList<String>>()
        var curChunk = mutableListOf<String>()
        diff.split("\n").forEach { line ->
            if(line.startsChunk()) {
                if(curChunk.isNotEmpty()) {
                    chunks.add(curChunk)
                    curChunk = mutableListOf<String>()
                }
                curChunk.add(line)
            } else if(curChunk.isNotEmpty()) {
                curChunk.add(line)
            }
        }
        if(curChunk.isNotEmpty()) {
            chunks.add(curChunk)
        }
        return chunks
    }

    val chunks = chunkify()
    val events = mutableListOf<Event>()

    class Hunk(
        val lineNum : Int,
        val oldLines : List<String>,
        val newLines : List<String>
    )

    fun hunkize(chunkWoHeader : List<String>, firstLineNum : Int) : List<Hunk> {
        var curOld = mutableListOf<String>()
        var curNew = mutableListOf<String>()
        var curLine = firstLineNum
        var hunks = mutableListOf<Hunk>()

        chunkWoHeader.forEach { line ->
            if(line.isOldLine()) {
                curOld.add(line)
            } else if(line.isNewLine()) {
                curNew.add(line)
            } else if(curOld.isNotEmpty() || curNew.isNotEmpty()) {
                hunks.add(Hunk(curLine, curOld, curNew))
                curLine += curNew.size + 1
                curOld = mutableListOf<String>()
                curNew = mutableListOf<String>()
            } else {
                curLine++
            }
        }
        if(curOld.isNotEmpty() || curNew.isNotEmpty()) {
            hunks.add(Hunk(curLine, curOld, curNew))
        }

        return hunks
    }

    fun stepHunk(hunk : Hunk) {
        val oldLen = hunk.oldLines.size
        val newLen = hunk.newLines.size
        val maxLen = max(oldLen, newLen)
        var lineNum = hunk.lineNum

        for (i in 0 until maxLen) {
            if(i < oldLen && i < newLen) {
                events += Event(
                    ChangeType.Change,
                    lineNum,
                    hunk.newLines[i].substring(1)
                )
                lineNum++
            } else if(i < oldLen) {
                events += Event(
                    ChangeType.Remove,
                    lineNum,
                    null
                )
            } else {
                events += Event(
                    ChangeType.Add,
                    lineNum,
                    hunk.newLines[i].substring(1)
                )
                lineNum++
            }
        }
    }

    fun stepChunk(chunk : List<String>) {
        val header = chunk[0]

        // format of header is
        //
        // @@ -old_line_num,cnt_lines_in_old_chunk, +new_line_num,cnt_lines_in_new_chunk
        //
        val (_, lineInfo, _) = header.split("@@")
        val offsets = lineInfo.trim().split(" ")

        // we only care about the new offset, since in the first chunk
        // of the file the new and old are the same, and since we add
        // and subtract lines as we go, we should stay in step with the
        // new offsets.
        val newOffset = offsets[1].split(",").map{
            abs(it.toInt())
        }.first()

        // a hunk is a group of contiguous - + lines
        val hunks = hunkize(chunk.subList(1, chunk.size), newOffset)

        hunks.forEach(::stepHunk)
    }

    chunks.forEach(::stepChunk)

    return events
}