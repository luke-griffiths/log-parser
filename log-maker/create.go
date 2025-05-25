package main

import (
	"bufio"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"time"
)

var animalSounds = map[string]string{
	"cow":   "moo",
	"pig":   "oink",
	"dog":   "woof",
	"cat":   "meow",
	"bird":  "squawk",
	"horse": "neigh",
	"goat":  "??",
	"mouse": "squeak",
	"sheep": "bahh",
}

type Entry struct {
	Container string    `json:"container"`
	Timestamp time.Time `json:"timestamp"`
	Msg       string    `json:"msg"`
	Level     string    `json:"level"`
	Happiness int       `json:"happiness"`
	ID        int       `json:"id"`
}

func createEntry(id int, animals []string) Entry {
	animalIndex := rand.IntN(len(animals))
	animal := animals[animalIndex]
	sound := animalSounds[animal]
	// with ~1% probability, the animal makes an atypical sound
	if rand.Float32() <= 0.01 {
		otherAnimal := animals[rand.IntN(len(animals))]
		sound = animalSounds[otherAnimal]
	}
	// make some levels appear less frequently
	randomNum := rand.IntN(10000)
	var level string
	switch {
	case randomNum == 0:
		level = "FATAL"
	case randomNum >= 1 && randomNum <= 10:
		level = "ERROR"
	case randomNum >= 11 && randomNum < 140:
		level = "WARN"
	case randomNum >= 140 && randomNum < 500:
		level = "INFO"
	default:
		level = "DEBUG"
	}

	return Entry{Container: animal,
		Timestamp: time.Now().UTC(),
		Msg:       sound,
		Level:     level,
		Happiness: rand.Int(),
		ID:        id}
}

func main() {
	args := os.Args
	numEntries, err := strconv.Atoi(args[1])
	if err != nil {
		panic(err)
	}
	slog.Info("creating log", "entries", numEntries)

	animals := make([]string, len(animalSounds))
	i := 0
	for animal := range animalSounds {
		animals[i] = animal
		i++
	}

	file, err := os.Create("example.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	bufferedWriter := bufio.NewWriter(file)
	defer bufferedWriter.Flush()

	encoder := json.NewEncoder(bufferedWriter)

	for i := range numEntries {
		if i%1000000 == 0 {
			slog.Info("logging", "entryNumber", i, "remaining", numEntries-i)
		}
		entry := createEntry(i, animals)
		err = encoder.Encode(entry)
		if err != nil {
			panic(err)
		}
	}
	slog.Info("done")

}
