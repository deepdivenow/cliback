package main

import "fmt"

var (
	cliBackVer = cliBackVersion{
		progName: "CliBack",
		majorVer: 0,
		minorVer: 9,
	}
	BuildVer = ""
)

type cliBackVersion struct {
	majorVer int
	minorVer int
	progName string
}

func (c *cliBackVersion) GetVersion() string {
	return fmt.Sprintf("%s version %d.%d %s", c.progName, c.majorVer, c.minorVer, BuildVer)
}
