package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"math"
	"os"
	"strings"
	"time"
)

type Popular struct {
	Id          uint64    `json:"id"`
	Ip          string    `json:"ip"`
	EpisodeId   uint32    `json:"episodeId"`
	CreatedDate time.Time `json:"createdDate"`
}

const PerPage int = 10000

func main() {
	var (
		filename     string
		start        string
		end          string
		totalCount   int
		totalPageNum int
		pageNum      int = 0
	)

	username := flag.String("u", "root", "username")
	password := flag.String("p", "", "password")
	dryRun := flag.Bool("dryRun", false, "If true, data won't be deleted.")
	flag.Parse()

	if len(flag.Args()) < 1 {
		log.Println("Usage: popular-backup -u=root -p=password targetDay(e.g 2015-08-26)")
		os.Exit(1)
	}

	targetDay := flag.Args()[0]
	start = targetDay + " 00:00:00"
	end = targetDay + " 23:59:59"
	dir, _ := os.Getwd()
	filename = fmt.Sprintf("%s/popular.%s.json.log", strings.Replace(dir, " ", "\\ ", -1), targetDay)

	log.Printf("[%s] Start with %s target day.\n", targetDay, targetDay)

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@/comicpanda?parseTime=true", *username, *password))
	isError(err)
	defer db.Close()

	err = db.QueryRow("SELECT count(*) from popular where created_date between ? and ?", start, end).Scan(&totalCount)
	isError(err)

	log.Printf("[%s] %d rows affected.\n", targetDay, totalCount)

	if totalCount > 0 {
		totalPageNum = int(math.Ceil(float64(totalCount) / float64(PerPage)))

		f, err := os.Create(filename)
		isError(err)
		defer f.Close()

		// Write result to file.
		for ; pageNum < totalPageNum; pageNum++ {
			stmt, err := db.Prepare("SELECT id, ip, episode_id, created_date FROM popular WHERE created_date  between ? and ? limit ?,?")
			isError(err)
			defer stmt.Close()

			rows, err := stmt.Query(start, end, pageNum*PerPage, PerPage)
			isError(err)
			defer rows.Close()

			pop := Popular{}

			for rows.Next() {
				rows.Scan(&pop.Id, &pop.Ip, &pop.EpisodeId, &pop.CreatedDate)
				j, _ := json.Marshal(pop)
				_, err := f.WriteString(string(j))
				isError(err)
				f.WriteString("\n")
			}
		}
		f.Sync()

		// Delete data.
		if !*dryRun {
			stmt, err := db.Prepare("DELETE FROM popular WHERE created_date between ? and ?")
			isError(err)

			res, err := stmt.Exec(start, end)
			isError(err)
			rows, err := res.RowsAffected()
			isError(err)
			log.Printf("[%s] %d rows deleted.\n", targetDay, rows)
		}
	}

	log.Printf("[%s] End.", targetDay)
}

func isError(err error) {
	if err != nil {
		log.Fatal(err)
		panic(err.Error())
	}
}
