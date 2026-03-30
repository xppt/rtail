package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"rtail/run_cli"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

func main() {
	run_cli.RunCli(entryPoint)
}

func entryPoint() {
	host := flag.String("h", "localhost", "")
	port := flag.String("p", "6379", "")
	user := flag.String("user", "", "")
	pass := flag.String("pass", "", "")
	start := flag.String("start", "$", "")
	end := flag.String("end", "", "")
	countFlag := flag.String("count", "", "")
	followFlag := flag.Bool("follow", false, "")
	base64Flag := flag.Bool("base64", false, "")
	flag.Parse()

	if flag.NArg() != 1 {
		run_cli.Die("unexpected args")
	}

	streamKey := flag.Arg(0)

	var limit *int
	if *countFlag != "" {
		countInt, err := strconv.Atoi(*countFlag)
		run_cli.DieOnError(err, "bad count")
		limit = &countInt
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

	xInfo, err := redisClient.XInfoStream(ctx, streamKey).Result()
	run_cli.DieOnError(err, "xinfo stream for a key failed")

	var nextBoundId string
	if (*start == "$") {
		nextBoundId = xInfo.LastGeneratedID
	} else if (*start == "+") {
		nextBoundId = prevMsgId(parseMsgId(xInfo.LastGeneratedID)).String()
	} else {
		nextBoundId = prevMsgId(parseMsgId(*start)).String()
	}

	if endId == nil && !*followFlag {
		parsedEndId := parseMsgId(xInfo.LastGeneratedID)
		endId = &parsedEndId
	}

	for {
		streamMsgs := xReadChunk(ctx, redisClient, streamKey, nextBoundId, *followFlag)

		for _, msg := range streamMsgs {
			nextBoundId = msg.ID
			msgIdParsed := parseMsgId(msg.ID)

			if endId != nil && compareMsgId(msgIdParsed, *endId) > 0 {
				return
			}

			body := make(map[string]string)
			for key, value := range msg.Values {
				b, ok := value.(string)
				if !ok {
					continue
				}

				if *base64Flag {
					body[key] = base64.StdEncoding.EncodeToString([]byte(b))
				} else {
					body[key] = b
				}
			}

			out, _ := json.Marshal(map[string]any{
				"id": nextBoundId,
				"ts": time.UnixMilli(int64(msgIdParsed.ts)).Format(time.RFC3339Nano),
				"body": body,
			})
			fmt.Println(string(out))
			seen++

			if limit != nil && seen >= *limit {
				return
			}
		}
	}
}

func xReadChunk(
		ctx context.Context, redisClient *redis.Client,
		streamKey string, nextBoundId string, follow bool,
) []redis.XMessage {

	blockTime := time.Duration(-1)
	if follow {
		blockTime = time.Second
	}

	readRes, err := redisClient.XRead(ctx, &redis.XReadArgs{
		Streams: []string{streamKey, nextBoundId},
		Block: blockTime,
		Count: 1000,
	}).Result()

	if err == redis.Nil {
		return nil
	}

	run_cli.DieOnError(err, "failed to read")

	if len(readRes) == 0 {
		return nil
	} else if len(readRes) != 1 {
		run_cli.DieF("unexpected xread resp size")
	}

	return readRes[0].Messages
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
	run_cli.DieOnError(err, "bad msg ts")

	var index uint64
	if len(parts) >= 2 {
		index, err = strconv.ParseUint(parts[1], 10, 64)
		run_cli.DieOnError(err, "bad msg index")
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
	if value.ts > 0 {
		return msgIdParsed{ts: value.ts - 1, seq: math.MaxUint64}
	}
	return value
}
