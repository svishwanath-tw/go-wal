package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

type WALog struct {
	filePath        string
	file            *os.File
	fileWriteOffset int64
	messageOffsets  map[int]int64
	latestOffset    int
}

var EmptyLog = &WALog{}

func NewWALog(path string) *WALog {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("[ERROR]: Unable to open file %v: %v", path, err)
	}

	info, err := file.Stat()
	if err != nil {
		log.Fatalf("[ERROR]: Unable to determine file(%v) stats %v", path, err)
	}

	log := &WALog{path, file, info.Size(), make(map[int]int64), 0}
	log.rebuildOffsets()
	return log
}

func (l *WALog) Read(offset int) ([]byte, []byte) {
	fileOffset := l.messageOffsets[offset]

	l.file.Seek(fileOffset, os.SEEK_SET)
	messageLength := int64(0)
	binary.Read(l.file, binary.LittleEndian, &messageLength)

	key := readWithLength(l.file)
	data := readWithLength(l.file)
	return key, data
}

func readWithLength(file *os.File) []byte {
	messageLength := int64(0)
	binary.Read(file, binary.LittleEndian, &messageLength)
	message := make([]byte, messageLength, messageLength)
	file.Read(message)
	return message
}

func (l *WALog) ReadKey(key []byte) []byte {
	return []byte{}
}

func (l *WALog) Append(key, data []byte) int {
	messageBytes := bytes.NewBuffer([]byte{})
	writeWithLength(key, messageBytes)
	writeWithLength(data, messageBytes)

	l.file.Seek(int64(l.fileWriteOffset), os.SEEK_SET)
	messageLength := int64(messageBytes.Len())
	err := binary.Write(l.file, binary.LittleEndian, &messageLength)
	if err != nil {
		log.Fatalf("[ERROR] Unable to write message length: %v", err)
	}
	l.file.Write(messageBytes.Bytes())
	l.latestOffset++
	l.messageOffsets[l.latestOffset] = l.fileWriteOffset
	l.fileWriteOffset += int64(messageBytes.Len() + 8)
	return l.latestOffset
}

func writeWithLength(bytes []byte, buffer *bytes.Buffer) {
	length := int64(len(bytes))
	err := binary.Write(buffer, binary.LittleEndian, length)
	if err != nil {
		log.Fatalf("[ERROR] Unable to write data length: %v", err)
	}
	buffer.Write(bytes)
}

func (l *WALog) rebuildOffsets() {
	if len(l.messageOffsets) == 0 && l.fileWriteOffset != 0 {
		log.Println("[DEBUG] Rebuilding message offsets")
		l.file.Seek(0, os.SEEK_SET)
		ptr := int64(0)
		offset := 0
		for ptr < l.fileWriteOffset {
			l.messageOffsets[offset] = ptr
			messageLength := int64(0)
			binary.Read(l.file, binary.LittleEndian, &messageLength)
			l.file.Seek(messageLength, os.SEEK_CUR)
			ptr += (messageLength + 8)
			offset++
		}
		l.latestOffset = offset
	}
}

func (l *WALog) Close() error {
	return l.file.Close()
}

func (l *WALog) ShowAllMessages() {
	for offset := 0; offset < l.latestOffset; offset++ {
		key, data := l.Read(offset)
		fmt.Printf("offset: %v, key: %v, data: %v\n", offset, string(key), string(data))
	}
}
