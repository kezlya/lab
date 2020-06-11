package main

import (
	"flag"
	"fmt"
	"log"
	"main/models"
)

var action, host, index *string

func main() {
	parseFlags()
	models.ConnectElastic(*host)

	if *action == "push" {
		fmt.Println("pushing objects to", host)
		models.InsertDocsFromFolder(*index, "upload", 100)
		fmt.Println("done")
	} else if *action == "pull" {
		fmt.Println("pulling objects from", host)
		models.SaveDocsToFolder(*index, "download", 100)
		fmt.Println("done")
	}
}

func parseFlags() {
	host = flag.String("host", "", "")
	action = flag.String("action", "", "")
	index = flag.String("index", "", "")
	flag.Parse()
	if *host == "" {
		log.Fatal("host not set")
	}
	if *action == "" {
		log.Fatal("action not set")
	}
	if *index == "" {
		log.Fatal("index not set")
	}
}
