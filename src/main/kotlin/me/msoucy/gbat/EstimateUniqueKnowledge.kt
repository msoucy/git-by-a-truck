package me.msoucy.gbat

import kotlin.math.abs
import kotlin.math.min

typealias Knowledge = MutableMap<String, Double>

fun sequentialKnowledge(lines: List<FileData>, churnConstant: Double): List<FileData> {
    fun createKnowledge(devUniq: Knowledge, dev: String, adj: Double) {
        devUniq[dev] = devUniq.getValue(dev) + adj
    }

    fun destroyKnowledge(devUniq: Knowledge, totKnowledge: Double, adj: Double) {
        var pctToDestroy = 0.0
        if (totKnowledge != 0.0) {
            pctToDestroy = abs(adj) / totKnowledge
        }
        devUniq.forEach { (devs, k) ->
            devUniq[devs] = k * (1 - pctToDestroy)
        }
    }

    fun shareKnowledgeGroup(dev: String, exploded: List<String>, pctToShare: Double, devUniq: Knowledge) {
        val oldShared = exploded.joinToString("\u0000")
        // Sort so that dev names stay alphabetical
        val newKey = (exploded + dev).sorted()
        val newShared = newKey.joinToString("\u0000")
        val groupKnowledge = devUniq.getValue(oldShared)
        val amtToShare = pctToShare * groupKnowledge
        devUniq[oldShared] = devUniq.getValue(oldShared) - amtToShare
        devUniq[newShared] = devUniq.getValue(newShared) + amtToShare
    }

    fun distributeShared(dev: String, sharedKnowledge: Double, totKnowledge: Double, devUniq: Knowledge) {
        var pctToShare = if (totKnowledge != 0.0) { sharedKnowledge / totKnowledge } else 0.0
        devUniq.keys.forEach { sharedKey ->
            val exploded = sharedKey.split('\u0000')
            if (dev !in exploded) {
                shareKnowledgeGroup(dev, exploded, pctToShare, devUniq)
            }
        }
    }

    fun estimateUniq(fd: FileData): FileData {
        var totKnowledge = 0.0
        val devUniq = mutableMapOf<String, Double>().withDefault { 0.0 }
        fd.devExp.forEach { (dev: String, added: Int, deleted: Int) ->
            val adjustment = (added - deleted).toDouble()
            if (adjustment > 0) {
                createKnowledge(devUniq, dev, adjustment)
            } else if (adjustment < 0) {
                destroyKnowledge(devUniq, totKnowledge, adjustment)
            }
            val churn = min(added, deleted)
            if (churn != 0) {
                val newKnowledge = churn.toDouble() * churnConstant
                val sharedKnowledge = churn.toDouble() - newKnowledge
                distributeShared(dev, sharedKnowledge, totKnowledge, devUniq)
                createKnowledge(devUniq, dev, newKnowledge)
            }
            totKnowledge += adjustment + (churn * churnConstant)
        }
        fd.devUniq = devUniq.map { (sharedKey, shared) ->
            sharedKey.split('\u0000') to shared
        }
        fd.totKnowledge = totKnowledge
        return fd
    }

    return lines.map { fd ->
        estimateUniq(fd)
    }
}
