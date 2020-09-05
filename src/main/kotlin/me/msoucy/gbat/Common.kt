package me.msoucy.gbat

fun safeAuthorName(author : String) = author.replace(',' '_').replace(':', '_')

typealias DevShared = List<Pair<List<String>, Double>>
typealias DevExp = List<Triple<String, Int, Int>>

fun parseDevShared(s : String) : DevShared =
    s.split(',').map { ddv ->
        val segs = ddv.split(':')
        val k = segs.slice(0..segs.size)
        val v = segs.last().toDouble()
        k to v
    }

fun devSharedToStr(devShared : DevShared) =
    devShared.map { (devs, shared) ->
        "${devs.joinToString(":")}:${shared}"
    }.joinToString(",")

fun parseDevExpStr(s : String, numFunc : (String) -> Int) =
    s.split(",").map {
        it.split(":")
    }.map {
        Triple(it[0], numFunc(it[1]), numFunc(it[2]))
    }

fun devExpToStr(devs : DevExp) = 
    devs.map {
        listOf(it.first,
               it.second.toString(),
               it.third.toString()).joinToString(":")
    }.joinToString(",")

fun projectName(fname : String) = fname.split(":").first()

class FileData(line: String) {

    constructor() : this(":::::") {}
    private val fields = line.trim().split("\t")
    var fname = fields[0]
    var project = projectName(fname)
    var cntLines = fields[1].toInt()
    var totKnowledge = fields[3].toDouble()

    var devExp = parseDevExpStr(fields[2], ::toDouble)
    var devUniq = parseDevShared(fields[4])
    var devRisk = parseDevShared(fields[5])

    fun asLine() = listOf(
        fname,
        cntLines.toString(),
        devExpToStr(devExp),
        totKnowledge.toString(),
        devSharedToStr(devUniq),
        devSharedToStr(devRisk)
        ).joinToString("\t")

    override fun toString() =
        "fname: $fname, cntLines: $cntLines, devExp: $devExp, totKnowledge: $totKnowledge, devUniq: $devUniq, risk: $devRisk"
}
