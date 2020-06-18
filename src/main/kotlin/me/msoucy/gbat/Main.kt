package me.msoucy.gbat

import java.io.File
import java.lang.System
import kotlin.text.startsWith

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.InvalidArgumentException
import com.xenomachina.argparser.default
import com.xenomachina.argparser.mainBody

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
	"\\.js$"
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
		help="Regular expression to determine which files should be included in calculations.",
		initialValue=DEFAULT_INTERESTING_RES
		) { this }
	val not_interesting by parser.adding("--not-interesting", "-N",
		help="Regular expression to determine which files should not be included in calculations.")
	val case_sensitive by parser.flagging("Use case sensitive regexps when determining interesting files (default is case-insensitive)")
	val departed by parser.storing("--departed-file", "-D", help="File listing departed devs, one per line").default<String?>(null)
	val risk_file by parser.storing("--bus-risk-file", help="File of dev=float lines (e.g. ejorgensen=0.4) with custom bus risks for devs").default<String?>(null)
	val default_bus_risk by parser.storing("--default-bus-risk", help="Default risk that a dev will be hit by a bus in your analysis timeframe (defaults to 0.1).").default(0.1)

	// Multiprocessing options
	val num_git_procs by parser.storing("--num-git-procs", help="The number of git processes to run simultaneously (defaults to 3)") { toInt() }.default(3)
	val num_analyzer_procs by parser.storing("--num-analyzer-procs", help="The number of analyzer processes to run (defaults to 3)") { toInt() }.default(3)

	// Tuning options
	val risk_threshold by parser.storing("--risk-threshold", help="Threshold past which to summarize risk (defaults to default bus risk cubed)") {toDouble()}.default<Double?>(null)
	val creation_constant by parser.storing("--knowledge-creation-constant", help="How much knowledge a changed line should create if a new line creates 1 (defaults to 0.1)") {toDouble()}.default<Double?>(null)

	// Misc options
	val git_exe by parser.storing("--git-exe", help="Path to the git executable", transform=::validateGit).default("git").addValidator { validateGit(value) }
	val verbose by parser.flagging("--verbose", "-v", help="Print comforting output")
	val output by parser.storing("Output directory for data files and html summary (defaults to \"output\"), error if already exists").default("output")

	// Directory
	val root by parser.positional("The root directory to inspect")
}

fun main(args: Array<String>) = mainBody {
	ArgParser(args).parseInto(::GbatArgs).run {
		println("Hello world")
		println(validateGit(git_exe))
	}
}
