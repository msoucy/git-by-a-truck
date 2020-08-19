package me.msoucy.gbat

import java.io.File
import java.io.IOException
import java.util.concurrent.TimeUnit

fun <T> Iterable<T>.copyOf(): List<T> = mutableListOf<T>().also { it.addAll(this) }
fun <T> Iterable<T>.mutableCopyOf() = mutableListOf<T>().also { it.addAll(this) }

fun List<String>.runCommand(workingDir: File): Pair<String?, String?> {
    try {
        val proc = ProcessBuilder(*this.toTypedArray())
                .directory(workingDir)
                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                .redirectError(ProcessBuilder.Redirect.PIPE)
                .start()

        proc.waitFor(5, TimeUnit.SECONDS)
        return Pair(proc.inputStream.bufferedReader().readText(),
                    proc.errorStream.bufferedReader().readText())
    } catch (e: IOException) {
        e.printStackTrace()
        return Pair(null, null)
    }
}
