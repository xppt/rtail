rtail
===
Redis stream tail tool.

Usage
---
```
rtail
    [-h <host>] [-p <port>]
    [[--user <user>] --pass <pass>]
    [--start <start-msg-id>] [--end <end-msg-id>]
    [--count <max-count>]
    stream-key
```

Both `start-msg-id` and `end-msg-id` boundaries are inclusive.
`rtail` will block on new messages (unless `--end` or `--count` reached).

Output is jsonlines of the following objects:
```
{
    "id":"1763543271199-0",
    "ts":"2025-11-19T12:07:51.199+03:00",
    "body": {"body_key": <base64-body-value>}
}
```

Examples
---
```
$ rtail --count 3 --start 1763543271199 my_stream_name

<jsonline>
<jsonline>
<jsonline>
```
