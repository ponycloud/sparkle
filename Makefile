#!/usr/bin/make -f

# Generic documentation files.
mds   := $(shell find * -name '*.md')
dias  := $(shell find * -name '*.dia')
htmls := $(mds:.md=.html)
pngs  := $(dias:.dia=.png)

# Database diagrams to convert to documentation.
dbdias  := doc/database.dia
dbmds   := $(dbdias:.dia=.md)
dbhtmls := ${htmls} $(dbmds:.md=.html)
dbpngs  := $(shell cat ${dbdias} \
		   | grep -A1 '^      <dia:attribute name="name"' \
		   | grep -o '\#.*\#' \
		   | sed -re 's@\#(.*)\#@doc/\1.table.png@')

mds   := ${mds} ${dbmds}
htmls := ${htmls} ${dbhtmls}
pngs  := ${pngs} ${dbpngs}

all: doc

doc: ${htmls}

clean:
	${RM} ${htmls} ${pngs} ${dbmds}

%.png: %.dia
	dia -t png -e $@ $<

${dbpngs}: ${dbmds}
${dbmds}: ${dbdias} doc/tools/convert.py
	cd doc && tools/convert.py ../$< >../$@

%.html: %.md doc/tools/render doc/tools/style.css ${pngs}
	doc/tools/render $<

.PRECIOUS: ${htmls} ${pngs}

# EOF
