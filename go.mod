module main

go 1.21

replace github.com/srmadden/godb => ./godb

require (
	github.com/chzyer/readline v1.5.1
	github.com/srmadden/godb v0.0.0-00010101000000-000000000000
)

require (
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2 // indirect
	golang.org/x/sys v0.20.0 // indirect
)
