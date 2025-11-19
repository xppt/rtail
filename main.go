package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"math"
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

	var nextBoundId string
	if *start == "" {
		nextBoundId = "$"
	} else {
		nextBoundId = prevMsgId(parseMsgId(*start)).String()
	}

	var endId *msgIdParsed
	if *end != "" {
		parsedEndId := parseMsgId(*end)
		endId = &parsedEndId
	}

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
			Streams: []string{streamKey, nextBoundId},
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

				nextBoundId = msg.ID
				msgIdParsed := parseMsgId(msg.ID)

				if endId != nil && compareMsgId(msgIdParsed, *endId) > 0 {
					return
				}

				body := make(map[string]string)
				for k, v := range msg.Values {
					b, ok := v.(string)
					if !ok {
						continue
					}
					body[k] = base64.StdEncoding.EncodeToString([]byte(b))
				}

				out, _ := json.Marshal(map[string]any{
					"id": nextBoundId,
					"ts": time.UnixMilli(int64(msgIdParsed.ts)).Format(time.RFC3339Nano),
					"body": body,
				})
				fmt.Println(string(out))
				seen++
			}
		}
	}
}

type msgIdParsed struct {
	ts uint64
	seq uint64
}

func (self msgIdParsed) String() string {
	return fmt.Sprintf("%v-%v", self.ts, self.seq)
}

func parseMsgId(value string) msgIdParsed {
	parts := strings.Split(value, "-")

	ts, err := strconv.ParseUint(parts[0], 10, 64)
	dieOnError(err, "bad msg ts")

	var index uint64
	if len(parts) >= 2 {
		index, err = strconv.ParseUint(parts[1], 10, 64)
		dieOnError(err, "bad msg index")
	} else {
		index = 0
	}

	return msgIdParsed{
		ts: ts,
		seq: index,
	}
}

func compareMsgId(left msgIdParsed, right msgIdParsed) int {
	if left.ts < right.ts {
		return -1
	}
	if left.ts == right.ts {
		if left.seq < right.seq {
			return -1
		}
		if left.seq == right.seq {
			return 0
		}
	}
	return 1
}

func prevMsgId(value msgIdParsed) msgIdParsed {
	if value.seq > 0 {
		return msgIdParsed{ts: value.ts, seq: value.seq - 1}
	}
	return msgIdParsed{ts: value.ts - 1, seq: math.MaxUint64}
}

func dieOnError(err error, msg string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", msg, err)
		os.Exit(1)
	}
}
