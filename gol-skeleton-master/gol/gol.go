package gol

// Params provides the details of how to run the Game of Life and which image to load.
type Params struct {
	Turns       int
	Threads     int
	ImageWidth  int
	ImageHeight int
	EngineAddress string
}

// Run starts the processing of Game of Life. It should initialise channels and goroutines.
func Run(p Params, events chan<- Event, keyPresses <-chan rune) {
	// If the engine address is not provided, use the default address.
	if p.EngineAddress == "" {
    p.EngineAddress = "127.0.0.1:8030"
	}

	ioFilename := make(chan string)
	ioOutput := make(chan uint8)
	ioInput := make(chan uint8)
	ioCommand := make(chan ioCommand)
	ioIdle := make(chan bool)

	ioChannels := ioChannels{
		command:  ioCommand,
		idle:     ioIdle,
		filename: ioFilename,
		output:   ioOutput,
		input:    ioInput,
	}
	go startIo(p, ioChannels)

	distributorChannels := distributorChannels{
		events:     events,
		ioCommand:  ioCommand,
		ioIdle:     ioIdle,
		ioFilename: ioFilename,
		ioOutput:   ioOutput,
		ioInput:    ioInput,
		keyPress: keyPresses,//马勒戈壁这个那么阴
	}
	distributor(p, distributorChannels)
}
