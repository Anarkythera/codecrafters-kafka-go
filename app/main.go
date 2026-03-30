package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

var SERVER_PORT = 9092

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// TODO: Uncomment the code below to pass the first stage
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", SERVER_PORT))
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	resp, err := handleConnection(conn)
	if err != nil {
		fmt.Println("Error: ", err.Error())
		os.Exit(-1)
	}

	fmt.Printf("Response %d\n", resp)
	conn.Write(resp)

}

func handleConnection(conn net.Conn) ([]byte, error) {

	msgHeader := make([]byte, 4)
	_, err := conn.Read(msgHeader)
	if err != nil {
		return nil, err
	}

	msgSize := int(binary.BigEndian.Uint32(msgHeader))
	requestBody := make([]byte, msgSize)
	_, err = conn.Read(requestBody)
	if err != nil {
		return nil, err
	}

	resp := []byte{0, 0, 0, 0}

	var buff bytes.Buffer
	correlationId := extractCorrelationId(requestBody)
	fmt.Printf("Correlation ID of the msg: %d \n", correlationId)

	err = binary.Write(&buff, binary.BigEndian, correlationId)
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
	}

	resp = append(resp, buff.Bytes()...)

	return resp, nil
}

func extractCorrelationId(buf []byte) int32 {

	// hardcoded indexes the first bytes are for request_api_key and request_api_version
	// correlation id is 4 bytes
	correlationId := int32(binary.BigEndian.Uint32(buf[4:9]))

	return correlationId
}
