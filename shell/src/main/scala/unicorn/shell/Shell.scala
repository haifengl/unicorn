/*******************************************************************************
 * (C) Copyright 2017 Haifeng Li
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package unicorn.shell

import scala.tools.nsc.interpreter.ILoop

import ammonite.ops.Path
import ammonite.runtime.Storage

/** Ammonite REPL based shell.
  *
  * @author Haifeng Li
  */
object AmmoniteREPL {
  val home = Path(System.getProperty("user.home")) / ".smile"
  val prompt = "smile> "
  val welcome =
    raw"""
       |                        . . . .
       |                        ,`,`,`,`,
       |  . . . .               `\`\`\`\;
       |  `\`\`\`\`,            ~|;!;!;\!
       |   ~\;\;\;\|\          (--,!!!~`!       .
       |  (--,\\\===~\         (--,|||~`!     ./
       |   (--,\\\===~\         `,-,~,=,:. _,//
       |    (--,\\\==~`\        ~-=~-.---|\;/J,       Welcome to the Unicorn Database
       |     (--,\\\((```==.    ~'`~/       a |         BigTable, Document and Graph
       |       (-,.\\('('(`\\.  ~'=~|     \_.  \              Full Text Search
       |          (,--(,(,(,'\\. ~'=|       \\_;>
       |            (,-( ,(,(,;\\ ~=/        \                  Haifeng Li
       |            (,-/ (.(.(,;\\,/          )
       |             (,--/,;,;,;,\\         ./------.
       |               (==,-;-'`;'         /_,----`. \
       |       ,.--_,__.-'                    `--.  ` \
       |      (='~-_,--/        ,       ,!,___--. \  \_)
       |     (-/~(     |         \   ,_-         | ) /_|
       |     (~/((\    )\._,      |-'         _,/ /
       |      \\))))  /   ./~.    |           \_\;
       |   ,__/////  /   /    )  /
       |    '===~'   |  |    (, <.
       |             / /       \. \
       |           _/ /          \_\
       |          /_!/            >_\
       |
       |  Welcome to Unicorn Shell; enter ':help<RETURN>' for the list of commands.
       |  Type ":quit<RETURN>" to leave the Unicorn Shell
       |  Version ${BuildInfo.version}, Scala ${BuildInfo.scalaVersion}, SBT ${BuildInfo.sbtVersion}, Built at ${BuildInfo.builtAtString}
       |===============================================================================
    """.stripMargin

  val imports =
    s"""
       |import java.util.{Date, UUID}
       |import java.time.{LocalDate, LocalTime, LocalDateTime}
       |import java.sql.Timestamp
       |import unicorn.json._
       |import unicorn.bigtable.hbase.HBase
       |import unicorn.unibase._
       |import unicorn.unibase.graph._
       |import unicorn.unibase.sql._
       |import unicorn.narwhal._
       |repl.prompt() = "unicorn> "
     """.stripMargin

  val repl = ammonite.Main(
    predefCode = imports,
    defaultPredef = true,
    storageBackend = new Storage.Folder(home),
    welcomeBanner = Some(welcome),
    verboseOutput = false
  )

  def run() = repl.run()
  def runCode(code: String) = repl.runCode(code)
  def runScript(path: Path) = repl.runScript(path, Seq.empty)
}