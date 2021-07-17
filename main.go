package main

import (
	"fmt"
	"os"
	"time"
)

func main() {
	walogFilepath := "/tmp/logfile"
	if len(os.Args) > 1 {
		walogFilepath = os.Args[1]
	}
	mutatedKey := "quite-another-key"

	walog := NewWALog(walogFilepath)

	walog.Append([]byte(mutatedKey), []byte("old-value"))
	walog.Append([]byte("a-key"), []byte("a-value"))
	walog.Append([]byte("another-key"), []byte("another-value"))
	walog.Append([]byte(mutatedKey), []byte("newer-value"))
	walog.Append([]byte("some-other-key"), []byte("some-other-value"))
	walog.Append([]byte(mutatedKey), []byte(fmt.Sprintf("quite-another-value %v", time.Now())))
	walog.ShowAllMessages()

	value, _ := walog.ReadKey([]byte(mutatedKey))
	fmt.Printf("Data of key(%v): %v\n", mutatedKey, string(value))
}
