package main

import (
	"bufio"
	"os"
)

func writeFile(fineName string, toWrite string) {
	// open output file
	fileObject, err := os.Create(fineName)
	if err != nil {
		panic(err)
	}
	// close fileObject on exit and check for its returned error
	defer func() {
		if err := fileObject.Close(); err != nil {
			panic(err)
		}
	}()
	// make a write buffer
	w := bufio.NewWriter(fileObject)

	// make a buffer to keep chunks that are read
	// write a chunk
	if _, err := w.Write([]byte(toWrite)); err != nil {
		panic(err)
	}

	if err = w.Flush(); err != nil {
		panic(err)
	}
}
