package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// TODO: Uncomment the code below to pass the first stage
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	handleConnection(conn)

}

func handleConnection(conn net.Conn) {

	buf := make([]byte, 4096)

	_, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
	}

	// response, message_size (4 bytes), correlation_id (4 bytes)
	// correlation_id hardcoded to 7 for now

	resp := []byte{0, 0, 0, 0}

	var buff bytes.Buffer
	correlationId := int32(7)

	err = binary.Write(&buff, binary.BigEndian, correlationId)

	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
	}

	resp = append(resp, buff.Bytes()...)

	conn.Write(resp)
	conn.Close()

}
