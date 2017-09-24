package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
)

func handleHello(w http.ResponseWriter, req *http.Request) {
	name := req.FormValue("name")
	if len(name) > 0 {
		fmt.Fprintf(w, "{\"message\": \"Hello %s\"}", name)
	} else {
		http.Error(w, "Error: name not provided", http.StatusBadRequest)
	}
}

type Reading struct {
	Timestamp string  `json:"timestamp"`
	Value     float64 `json:"value"` // if you really care about accuracy probably leave as string
}

type Readings struct {
	Sensor_id uint32    `json:"sensor_id"`
	Readings  []Reading `json:"readings"`
}

func getReading(w *http.ResponseWriter, req *http.Request) {
	sensorId := req.FormValue("sensor_id")
	if len(sensorId) > 0 {
		sId, err := strconv.ParseUint(sensorId, 10, 32)
		if err != nil {
			http.Error(*w, "invalid sensor id", http.StatusBadRequest)
		} else {
			// go makes deferring responses to different threads overly complicated
			// each handler automatically sets the header response code to 200 when it returns and nothing is done in its lifetime
			done := make(chan bool)
			readChannel <- ReadMessage{uint32(sId), w, &done}
			// keeps this goroutine on the call stack so that we don't write multiple times
			<-done
		}
	} else {
		http.Error(*w, "Error: sensor_id not provided", http.StatusBadRequest)
	}
}

func postReading(w *http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	var readings Readings
	err := decoder.Decode(&readings)
	if err != nil {
		http.Error(*w, "Error: malformed request", http.StatusBadRequest)
	} else {
		done := make(chan bool)
		writeChannel <- WriteMessage{readings, w, &done}
		<-done
	}
}

// use channels to make concurrent requests safe, i.e. to make partial reads impossible
type ReadMessage struct {
	sensorId uint32
	writer   *http.ResponseWriter
	done     *chan bool
}

type WriteMessage struct {
	readings Readings
	writer   *http.ResponseWriter
	done     *chan bool
}

var readChannel = make(chan ReadMessage)
var writeChannel = make(chan WriteMessage)

// called on a different thread, handles read/writing to files in a continuous loop
func readWriteRoutine(dataDir *string) {
	for {
		select {
		case readMessage := <-readChannel:
			w := readMessage.writer
			readings := read(readMessage.sensorId, dataDir)
			readingsString, _ := json.Marshal(readings)
			(*w).Write([]byte(readingsString))
			(*readMessage.done) <- true
		case readings := <-writeChannel:
			w := readings.writer
			err := write(readings.readings, dataDir)
			if err != nil {
				http.Error(*w, "Error Writing", http.StatusBadRequest)
			} else {
				(*w).WriteHeader(http.StatusCreated)
			}
			(*readings.done) <- true
		}
	}
}

// lower level write writes readings to a file
func write(readings Readings, dataDir *string) error {
	filename := fmt.Sprintf("%s%d", *dataDir, readings.Sensor_id)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
		return err
	} else {
		defer file.Close()
		writer := bufio.NewWriter(file)
		for _, reading := range readings.Readings {
			fmt.Fprintln(writer, reading.Timestamp, reading.Value)
		}
		writer.Flush()
		return nil
	}
}

// reads readings from the file it belongs to
func read(sensorId uint32, dataDir *string) *Readings {
	filename := fmt.Sprintf("%s%d", *dataDir, sensorId)
	file, err := os.Open(filename)
	if err != nil { // there's actually no error. sensor just doesnt exist
		return &Readings{Sensor_id: sensorId, Readings: []Reading{}}
	} else {
		defer file.Close()
		var readings []Reading
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			split := strings.Split(scanner.Text(), " ")
			value, _ := strconv.ParseFloat(split[1], 64)
			readings = append(readings, Reading{
				Timestamp: split[0],
				Value:     value,
			})
		}
		sort.Slice(readings, func(i, j int) bool { return readings[i].Timestamp < readings[j].Timestamp })
		return &Readings{sensorId, readings}
	}
}

func main() {
	port := os.Args[1]
	dataDir := os.Args[2]
	// separate routine to handle stateful reads and writes safely
	go readWriteRoutine(&dataDir)
	http.HandleFunc("/hello", handleHello)
	http.HandleFunc("/readings", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:
			getReading(&w, req)
		case http.MethodPost:
			postReading(&w, req)
		default:
			http.Error(w, "Undefined route", http.StatusBadRequest)
		}
	})

	println("data directory set to", dataDir)
	println("listening on port", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
