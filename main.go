package main

func main() {
	walog := NewWALog("./logfile")
	walog.Append([]byte("a-key"), []byte("a-value"))

	walog.Append([]byte("another-key"), []byte("another-value"))

	walog.Append([]byte("some-other-key"), []byte("some-other-value"))
	walog.Append([]byte("quite-another-key"), []byte("quite-another-value"))

	walog.ShowAllMessages()
}
