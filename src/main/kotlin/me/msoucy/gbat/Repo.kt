package me.msoucy.gbat

import java.io.File
import java.io.IOException

typealias Diff = String

class GitRepo(val projectRoot: File, val git_exe: String) {
    fun ls(): String {
        val cmd = listOf(
            git_exe,
            "ls-tree",
            "--full-tree",
            "--name-only",
            "-r",
            "HEAD",
            "--",
            projectRoot.absolutePath
        )
        val (out, _) = cmd.runCommand(projectRoot)
        return out ?: ""
    }

    fun root(): File {
        val cmd = listOf(
            git_exe,
            "rev-parse",
            "--show-toplevel"
        )
        val (out, _) = cmd.runCommand(projectRoot)
        return File((out ?: "").trim())
    }

    fun log(fname: File): List<Pair<String, Diff>> {
        val cmd = listOf(
            git_exe,
            "--no-pager",
            "log",
            "-z", // Null byte separate log entries
            "-w", // Ignore all whitespace
            "--follow", // Follow history through renames
            "--patience", // Use the patience diff algorithm
            "-p", // Show patches
            "--",
            fname.absolutePath
        )
        val (out, err) = cmd.runCommand(projectRoot)
        if (err != "") {
            System.err.println("Error from git log: " + err)
            throw IOException(err)
        }
        val logEntries = (out ?: "").split("\u0000").filter { it.trim().isNotEmpty() }
        return logEntries.map {
            val (header, diffLines) = splitEntryHeader(it)
            val diff = diffLines.joinToString("\n")
            val author = parseAuthor(header)
            author to diff
        }.filter {
            it.first != "" && it.second != ""
        }.reversed()
    }

    fun parseDevExperience(f: File): List<Triple<String, Int, Int>> {
        val cmd = listOf(
            git_exe,
            "log",
            "-z",
            "-w",
            "--follow",
            "--numstat",
            "--format=format:%an",
            f.absolutePath
        )
        val (out, err) = cmd.runCommand(projectRoot)
        if (err != "") {
            System.err.println("Error from git log: " + err)
            throw IOException(err)
        }
        return parseExperience(out.orEmpty())
    }

    private fun parseAuthor(header: List<String>): String {
        val segs = header.getOrNull(1)?.trim()?.split("\\s+".toRegex()) ?: listOf()
        return segs.subList(1, segs.size - 1).joinToString(" ")
    }

    private fun splitEntryHeader(entry: String): Pair<List<String>, List<String>> {
        val lines = entry.split("\n")
        if (lines.size < 2) {
            return Pair(listOf(), listOf())
        } else if (!lines.get(0).startsWith("commit")) {
            return Pair(listOf(), listOf())
        } else if (!lines.get(1).startsWith("Author")) {
            return Pair(listOf(), listOf())
        }
        var ind = 2
        while (ind < lines.size && !lines.get(ind).startsWith("diff")) {
            ind++
        }
        return Pair(lines.subList(0, ind).copyOf(),
                    lines.subList(ind, lines.size).copyOf())
    }

    private fun parseExperience(log: String): DevExp {
        val exp = mutableListOf<Triple<String, Int, Int>>()
        val entryLines = log.split("\u0000")
        var currentEntry = mutableListOf<String>()
        for (entryLine in entryLines) {
            if (entryLine.trim() != "") {
                currentEntry.addAll(entryLine.split("\n").map(String::trim))
            } else {
                var localEntry = currentEntry
                currentEntry = mutableListOf<String>()
                if (localEntry.size < 2) {
                    System.err.println("Weird entry, cannot parse: ${localEntry.joinToString("\n")}\n-----")
                    continue
                }
                val author = localEntry[0]
                val changes = localEntry[1].split("\\s".toRegex())
                val linesAdded = changes[0].toInt()
                val linesRemoved = changes[1].toInt()
                if (linesAdded != 0 && linesRemoved != 0) {
                    exp.add(Triple(author, linesAdded, linesRemoved))
                }
            }
        }
        return exp.reversed()
    }
}
