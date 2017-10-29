/*******************************************************************************
 * (C) Copyright 2015 ADP, LLC.
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

/** Unicorn shell.
  *
  * @author Haifeng Li
  */
class Shell extends ILoop {
  override def prompt = "unicorn> "
  override def printWelcome = echo(
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
  )
}
