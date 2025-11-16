package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

func main() {
	host := flag.String("h", "localhost", "")
	port := flag.String("p", "6379", "")
	user := flag.String("user", "", "")
	pass := flag.String("pass", "", "")
	start := flag.String("start", "", "")
	end := flag.String("end", "", "")
	countFlag := flag.String("count", "", "")
	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "missing key")
		os.Exit(1)
	}

	streamKey := flag.Arg(0)

	var limit *int
	if *countFlag != "" {
		countInt, err := strconv.Atoi(*countFlag)
		dieOnError(err, "bad count")
		limit = &countInt
	}

	var startId string
	if *start != "" {
		t, err := time.Parse(time.RFC3339, *start)
		dieOnError(err, "bad start time")
		startId = strconv.FormatInt(t.UnixMilli(), 10)
	} else {
		startId = strconv.FormatInt(time.Now().UnixMilli(), 10)
	}

	var endID *string
	if *end != "" {
		t, err := time.Parse(time.RFC3339, *end)
		dieOnError(err, "bad end time")
		s := strconv.FormatInt(t.UnixMilli(), 10)
		endID = &s
	}

	curID := startId
	seen := 0
	ctx := context.Background()

	redisClient := redis.NewClient(&redis.Options{
		Addr: *host + ":" + *port,
		Username: *user,
		Password: *pass,

		// Explicitly disable maintenance notifications
		// This prevents the client from sending CLIENT MAINT_NOTIFICATIONS ON
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})

	for {
		readRes, err := redisClient.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamKey, curID},
			Block: time.Second,
			Count: 1000,
		}).Result()

		if err != nil && err != redis.Nil {
			dieOnError(err, "failed to read")
		}
		if err == redis.Nil {
			continue
		}

		for _, stream := range readRes {
			for _, msg := range stream.Messages {
				if limit != nil && seen >= *limit {
					return
				}

				curID = msg.ID

				if endID != nil && curID > *endID {
					return
				}

				msgIdTsStr := strings.Split(curID, "-")[0]
				msgIdTs, err := strconv.ParseInt(msgIdTsStr, 10, 64)
				dieOnError(err, "bad msg id timestamp")
				msgTime := time.UnixMilli(msgIdTs)

				body := make(map[string]string)
				for k, v := range msg.Values {
					b, ok := v.(string)
					if !ok {
						continue
					}
					body[k] = base64.StdEncoding.EncodeToString([]byte(b))
				}

				out, _ := json.Marshal(map[string]any{
					"id": curID,
					"ts": msgTime.Format(time.RFC3339Nano),
					"body": body,
				})
				fmt.Println(string(out))
				seen++
			}
		}
	}
}

func dieOnError(err error, msg string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", msg, err)
		os.Exit(1)
	}
}
