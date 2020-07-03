package me.msoucy.gbat

import me.msoucy.gbat.models.RiskModel
import me.msoucy.gbat.models.SummaryModel

import java.io.File
import java.util.concurrent.Executors
import kotlin.math.pow
import kotlin.system.exitProcess
import kotlin.text.Regex
import kotlin.text.RegexOption
import kotlin.text.startsWith
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.InvalidArgumentException
import com.xenomachina.argparser.default
import com.xenomachina.argparser.mainBody

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

val REALLY_LONG_TIME = 864000
val DEFAULT_INTERESTING_RES = mutableListOf(
	"\\.java$",
	"\\.cs$",
	"\\.py$",
	"\\.c$",
	"\\.cpp$",
	"\\.h$",
	"\\.hpp$",
	"\\.pl$",
	"\\.perl$",
	"\\.rb$",
	"\\.sh$",
    "\\.js$",
    "\\.kt$"
)

fun validateGit(exe : String) : String {
    val os = System.getProperty("os.name")
    var fullexe = if(os.startsWith("Windows") && !exe.endsWith(".exe")) {
        exe + ".exe"
    } else exe
    var file = File(fullexe)
	if(file.canRead()) {
        return file.absolutePath
    }
    for(path in System.getenv("PATH").split(";")) {
        file = File(path, fullexe)
        if(file.canRead()) {
            return file.absolutePath
        }
    }
    throw InvalidArgumentException("Provided git executable does not exist")	
}

class GbatArgs(parser: ArgParser) {
	// Input options
	val interesting by parser.adding("--interesting", "-I",
		help="Regular expression to determine which files should be included in calculations.")
	val not_interesting by parser.adding("--not-interesting", "-N",
		help="Regular expression to determine which files should not be included in calculations.")
	val case_sensitive by parser.flagging("Use case sensitive regexps when determining interesting files (default is case-insensitive)")
	val departed by parser.storing("--departed-file", "-D", help="File listing departed devs, one per line", transform=::File).default<File?>(null)
	val risk_file by parser.storing("--bus-risk-file", help="File of dev=float lines (e.g. ejorgensen=0.4) with custom bus risks for devs", transform=::File).default<File?>(null)
	val default_bus_risk by parser.storing("--default-bus-risk", help="Default risk that a dev will be hit by a bus in your analysis timeframe (defaults to 0.1).") { toDouble() }.default(0.1)

	// Multiprocessing options
	val num_git_procs by parser.storing("--num-git-procs", help="The number of git processes to run simultaneously (defaults to 3)") { toInt() }.default(3)
	val num_analyzer_procs by parser.storing("--num-analyzer-procs", help="The number of analyzer processes to run (defaults to 3)") { toInt() }.default(3)

	// Tuning options
	val risk_threshold by parser.storing("--risk-threshold", help="Threshold past which to summarize risk (defaults to default bus risk cubed)") { toDouble() }.default<Double?>(null)
	val creation_constant by parser.storing("--knowledge-creation-constant", help="How much knowledge a changed line should create if a new line creates 1 (defaults to 0.1)") { toDouble() }.default(0.1)

	// Misc options
	val git_exe by parser.storing("--git-exe", help="Path to the git executable", transform=::validateGit).default("git").addValidator { validateGit(value) }
	val verbose by parser.flagging("--verbose", "-v", help="Print comforting output")
	val output by parser.storing("Output directory for data files and html summary (defaults to \"output\"), error if already exists").default("output")

	// Directory
    val project_root by parser.positional("The root directory to inspect")
}

fun main(args: Array<String>) = mainBody {
	ArgParser(args).parseInto(::GbatArgs).run {
        val outDir = File(output)
        if(outDir.isDirectory) {
            //throw InvalidArgumentException("Output directory already exists")
        }

        outDir.mkdirs()

        fun parse_interesting(theList : List<String>) =
            theList.map {
                if(case_sensitive) {
                    Regex(it)
                } else {
                    Regex(it, RegexOption.IGNORE_CASE)
                }
            }

        val riskThresh = risk_threshold ?: default_bus_risk.pow(3)
        val interesting_res = parse_interesting(if (interesting.isEmpty()) DEFAULT_INTERESTING_RES else interesting)
        val not_interesting_res = if (not_interesting.isEmpty()) listOf() else parse_interesting(not_interesting)

        val projectRootFile = File(project_root).also {
            if(!it.isDirectory)
                throw InvalidArgumentException("Provided project root does not exist")
        }

        val repo = GitRepo(projectRootFile, validateGit(git_exe))

        fun String.isInteresting() : Boolean {
            var hasInterest = interesting_res.any { it.containsMatchIn(this) }
            if(hasInterest) {
                hasInterest = !not_interesting_res.any { it.containsMatchIn(this) }
            }
            return hasInterest
        }

        fun GitRepo.interestingNames() = ls().split("\n").filter{ it.isInteresting() }
        val fnames = repo.interestingNames()

        if (fnames.isEmpty()) {
            System.err.println("No interesting files found, exiting.")
            exitProcess(1)
        }

        if (verbose) {
            System.err.println("Found ${fnames.size} interesting files")
        }

        val riskModel = RiskModel(riskThresh, default_bus_risk, risk_file, departed)

        val dbFname = File(outDir, "summary.db")
        dbFname.delete();
        val summaryDb = Database.connect("jdbc:sqlite:${dbFname.absolutePath}", driver="org.sqlite.JDBC")
        transaction(summaryDb) {
            addLogger(StdOutSqlLogger)
            // exec("PRAGMA journal_mode = OFF")
            // exec("PRAGMA synchronous = OFF")
        }
        val summaryModel = SummaryModel(summaryDb)

        runBlocking {
            flow {
                fnames.forEach { fname ->
                    emit(parseHistory(repo, projectRootFile, File(fname)))
                }
            }.map { history ->
                analyze(riskModel, creation_constant, history, verbose)
            }.collect { analysis ->
                summaryModel.summarize(analysis)
            }
        }

        renderSummary(projectRootFile, summaryModel, outDir)

        // Render summary
        System.err.println("Done, summary is in ${outDir}/index.html")
    }
}
