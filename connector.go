package main

import (
	"code.google.com/p/go.net/websocket"
	"encoding/json"
	_ "errors"
	"fmt"
	"github.com/jimlawless/cfg"
	"io"
	"log"
	"net/http"
)

// socket represents a socket connection
//
// io.ReadWriter is the standard read/write interface
// done is a channel to stop the socket
//
type socket struct {
	io.ReadWriter
	done chan bool
}

// Close adds a Close method to the socket making it a ReadWriteCloser
//
func (s socket) Close() error {
	s.done <- true
	return nil
}

// Message defines a websocket message
type Message struct {
	Id      int
	Command string
	Names   []string
	Tweet   string
	OldName string
	NewName string
	Hash    string
}

// Data is a map for command parameter to and from the controller
type Data map[string]interface{}

// loadConfig load the config file "bees.cfg""
func loadConfig() map[string]string {
	m := make(map[string]string)
	err := cfg.Load("21tweet.cfg", m)
	if err != nil {
		log.Fatal(err)
	}

	return m
}

// StartServer initiates the beeserver. It starts off all three components
//	Database
//	Controller
//	Connector
func StartServer() chan bool {
	config := loadConfig()
	if config == nil {
		panic(config)
	}

	doneChan := make(chan bool)

	requestChan := StartDatabase(config, doneChan)
	go StartConnector(config, requestChan, doneChan)

	return doneChan
}

// StartConnector starts up the websocket connector of the bee server
// 	config		settings from config file
// 	commandChan	channel to send commands to the controller
// 	doneChan	channel to signal end or get it signaled
func StartConnector(config map[string]string, requestChan chan Request, doneChan chan bool) {

	http.Handle(config["wsdir"], websocket.Handler(func(ws *websocket.Conn) {
		fmt.Println("New socket connection started ...")
		s := socket{ws, make(chan bool)}
		go translateMessages(s, requestChan)
		<-s.done
		fmt.Println("Socket connection gone ...")
	}))

	fmt.Println("21tweet connector started on ", config["wsaddress"]+":"+config["wsport"], ". Listening ...")

	err := http.ListenAndServe(config["wsaddress"]+":"+config["wsport"], nil)
	if err != nil {
		fmt.Println("Error: " + err.Error())
		doneChan <- true
	}
}

func translateMessages(s socket, requestChan chan Request) {
	decoder := json.NewDecoder(s)
	encoder := json.NewEncoder(s)

	var err error

	for {
		var message Message
		err = decoder.Decode(&message)
		if err != nil {
			fmt.Println("Connection error: ", err.Error())
			s.done <- true
			return
		}
		if message.Id == 0 {
			fmt.Println("connector.go/translateMessages: Message misses an id.")
			s.done <- true
			return
		}

		// open up a data channel for responses
		dataChan := make(chan Data)
		switch message.Command {
		case "checkNames":
			fallthrough
		case "changeName":
			fallthrough
		case "tweet":
			request := Request{
				request:   message.Command,
				dataChan:  dataChan,
				parameter: message,
			}

			requestChan <- request
			catchReturn(dataChan, encoder, message.Id)
		default:
			fmt.Println("connector.go/translateMessages: Unknown command", message.Command)
		}
	}
}

func catchReturn(dataChan chan Data, encoder *json.Encoder, id int) {
	select {
	case data := <-dataChan:
		fmt.Println("Got message back!", data)
		data["Id"] = id
		encoder.Encode(&data)
	}
}
