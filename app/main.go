package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"slices"
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
		os.Exit(1)
	}

	fmt.Printf("Response %d\n", resp)
	conn.Write(resp)

}

// Will only support api versions 0-4
func handleConnection(conn net.Conn) ([]byte, error) {

	msgHeader := make([]byte, 4)
	_, err := conn.Read(msgHeader)
	if err != nil {
		return nil, err
	}

	reqSize := int(binary.BigEndian.Uint32(msgHeader))
	request := make([]byte, reqSize)
	_, err = conn.Read(request)
	if err != nil {
		return nil, err
	}

	respHeader, err := createResponseHeader(request)
	if err != nil {
		return nil, err
	}

	respBody, err := createResponseBody(request)
	if err != nil {
		return nil, err
	}

	var respSize bytes.Buffer
	err = binary.Write(&respSize, binary.BigEndian, int32(len(respHeader)+len(respBody)))
	if err != nil {
		return nil, err
	}

	resp := slices.Concat(respSize.Bytes(), respHeader, respBody)

	return resp, nil
}

/*
 * Correlation ID int32
 *
 */
func createResponseHeader(request []byte) ([]byte, error) {

	var buff bytes.Buffer
	header := []byte{}
	correlationId := parseCorrelationId(request)

	fmt.Printf("Correlation ID of the msg: %d \n", correlationId)

	err := binary.Write(&buff, binary.BigEndian, correlationId)
	if err != nil {
		return nil, err
	}

	header = append(header, buff.Bytes()...)

	return header, nil
}

/*
 * has the following structure
 * error_code int16 (0 if successful request)
 * api_keys array int8 len + 1
 *   api_key int16
 *   min_version int16 0
 *   max_version int16 4
 *   tag_buffer int8 0
 * throttle_time_ms int32 0
 * tag_buffer int8 0
 *
 */
func createResponseBody(request []byte) ([]byte, error) {
	var buff bytes.Buffer

	apiVersion := parseAPIVersion(request)
	responseErrorCode := msgErrorCode(apiVersion)
	err := binary.Write(&buff, binary.BigEndian, responseErrorCode)
	if err != nil {
		return nil, err
	}

	apiKey, err := parseAPIKeys(request)
	if err != nil {
		return nil, err
	}

	trottleTimeMs, err := parseThrottleTimeMs(request)
	if err != nil {
		return nil, err
	}

	tagBuffer, err := parseTagBuffer()
	if err != nil {
		return nil, err
	}

	body := slices.Concat(buff.Bytes(), apiKey, trottleTimeMs, tagBuffer)
	return body, nil
}

/*
 * Currently hardcoded to 0
 */
func parseTagBuffer() ([]byte, error) {
	return []byte{0}, nil
}

/*
 * Currently hardcoded to 0
 */
func parseThrottleTimeMs(request []byte) ([]byte, error) {
	var buff bytes.Buffer
	throttleTimeMs := []byte{}
	err := binary.Write(&buff, binary.BigEndian, int32(0))
	if err != nil {
		return nil, err
	}

	throttleTimeMs = append(throttleTimeMs, buff.Bytes()...)

	return throttleTimeMs, nil

}

/*
 * Min version is 0
 * Max version is 4
 */
func parseAPIKeys(request []byte) ([]byte, error) {
	var buff bytes.Buffer
	apiKeys := []byte{}

	//hardcoded for now is always len(apikeys)+1
	apiKeysLength := int8(2)
	err := binary.Write(&buff, binary.BigEndian, apiKeysLength)
	if err != nil {
		return nil, err
	}

	apiKey := int16(18)
	err = binary.Write(&buff, binary.BigEndian, apiKey)
	if err != nil {
		return nil, err
	}

	minVersion := int16(0)
	err = binary.Write(&buff, binary.BigEndian, minVersion)
	if err != nil {
		return nil, err
	}

	maxVersion := int16(4)
	err = binary.Write(&buff, binary.BigEndian, maxVersion)
	if err != nil {
		return nil, err
	}

	tagBuffer, err := parseTagBuffer()
	if err != nil {
		return nil, err
	}

	err = binary.Write(&buff, binary.BigEndian, tagBuffer)
	if err != nil {
		return nil, err
	}

	apiKeys = append(apiKeys, buff.Bytes()...)
	return apiKeys, nil
}

func msgErrorCode(apiVersion int16) int16 {

	var errorCode int16

	if 0 <= apiVersion && apiVersion <= 4 {
		errorCode = 0
	} else {
		errorCode = 35
	}

	return errorCode
}

func parseAPIVersion(requestBody []byte) int16 {

	apiVersion := int16(binary.BigEndian.Uint16(requestBody[2:4]))
	return apiVersion

}

func parseCorrelationId(buf []byte) int32 {

	// hardcoded indexes the first bytes are for request_api_key and request_api_version
	// correlation id is 4 bytes
	correlationId := int32(binary.BigEndian.Uint32(buf[4:8]))

	return correlationId
}
