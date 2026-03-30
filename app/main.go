package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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

/*
 * all resp or req msg have a size field of 32bit
 */
func handleConnection(conn net.Conn) ([]byte, error) {

	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)

	/*
	 * TODO improvements
	 * read first 4 bytes and then read that number instead of until EOF
	 * handle tiemout of conn
	 */
	for {
		n, err := conn.Read(tmp)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		buf = append(buf, tmp[:n]...)
	}

	// response, message_size (4 bytes), correlation_id (4 bytes)
	// msg_size hardcoded to 0 now

	resp := []byte{0, 0, 0, 0}

	var buff bytes.Buffer
	correlationId := extractCorrelationId(buf)
	fmt.Printf("Correlation ID of the msg: %d \n", correlationId)

	err := binary.Write(&buff, binary.BigEndian, correlationId)
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
	}

	resp = append(resp, buff.Bytes()...)

	return resp, nil
}

func extractCorrelationId(buf []byte) int32 {

	correlationId := int32(binary.BigEndian.Uint32(buf[8:12]))

	return correlationId
}
