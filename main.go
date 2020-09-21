package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Popular struct {
	Id          uint64    `json:"id"`
	Ip          string    `json:"ip"`
	EpisodeId   uint32    `json:"episodeId"`
	CreatedDate time.Time `json:"createdDate"`
}

type SlackPayload struct {
	Text      string `json:"text"`
	Username  string `json:"username"`
	IconEmoji string `json:"icon_emoji"`
}

const PerPage int = 10000
const SlackAPIUrl string = "https://hooks.slack.com/services/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"

var slackNotification bool

func main() {
	var (
		query        string
		filename     string
		start        string
		end          string
		totalCount   int
		totalPageNum int
		pageNum      int = 0
	)

	hostForReading := flag.String("rh", "localhost", "host for reading")
	hostForDeleting := flag.String("dh", "localhost", "host for deleting")
	baseDir := flag.String("baseDir", "~", "base dir(without last /) e.g. /backup")
	table := flag.String("table", "popular", "Popular table name")
	username := flag.String("u", "root", "username")
	password := flag.String("p", "", "password")
	dryRun := flag.Bool("dryRun", false, "If true, data won't be deleted")

	flag.Parse()
	log.SetOutput(os.Stdout)

	slackNotification = !*dryRun
	if len(flag.Args()) < 1 {
		log.Println("Usage: popular-backup -u=root -p=password targetDay(e.g 2015-08-26)")
		os.Exit(1)
	}

	targetDay := flag.Args()[0]
	start = targetDay + " 00:00:00"
	end = targetDay + " 23:59:59"
	filename = fmt.Sprintf("%s/popular.%s.json.log", *baseDir, targetDay)

	log.Printf("[%s] Start with %s target day.\n", targetDay, targetDay)

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:3306)/comicpanda?parseTime=true", *username, *password, *hostForReading))
	isError(err)
	defer db.Close()

	err = db.QueryRow("SELECT count(*) FROM "+*table+" WHERE created_date BETWEEN ? AND ?", start, end).Scan(&totalCount)
	isError(err)

	log.Printf("[%s] %d rows affected.\n", targetDay, totalCount)

	if totalCount > 0 && !*dryRun {
		totalPageNum = int(math.Ceil(float64(totalCount) / float64(PerPage)))

		f, err := os.Create(filename)
		isError(err)
		defer f.Close()

		query = "SELECT id, ip, episode_id, created_date FROM " + *table + " WHERE created_date  BETWEEN ? AND ? limit ?,?"
		// Write result to file.
		for ; pageNum < totalPageNum; pageNum++ {
			stmt, err := db.Prepare(query)
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
			dbForDeleting, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:3306)/comicpanda?parseTime=true", *username, *password, *hostForDeleting))
			defer dbForDeleting.Close()
			isError(err)

			stmt, err := dbForDeleting.Prepare("DELETE FROM " + *table + " WHERE created_date between ? and ?")
			isError(err)

			res, err := stmt.Exec(start, end)
			isError(err)
			rows, err := res.RowsAffected()
			isError(err)
			log.Printf("[%s] %d rows deleted.\n", targetDay, rows)
		}
	}

	log.Printf("[%s] End.\n", targetDay)
}

func isError(err error) {
	if err != nil {
		if slackNotification {
			notifyToSlack(err.Error())
		}
		log.Fatal(err)
	}
}

func notifyToSlack(msg string) {
	payload := SlackPayload{Text: msg, Username: "I'm Popular Backup Bot", IconEmoji: ":construction_worker:"}
	j, _ := json.Marshal(payload)
	data := url.Values{}
	data.Set("payload", string(j))
	resp, err := http.PostForm(SlackAPIUrl, data)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
}
