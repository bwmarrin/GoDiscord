package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/bwmarrin/discordgo"
)

// Flags
var (
	GuildID  = flag.String("guild", "", "Test guild ID")
	BotToken = flag.String("token", "", "Bot token")
)

const timeout time.Duration = time.Second * 10

var games map[string]time.Time = make(map[string]time.Time)

func main() {
	flag.Parse()
	s, _ := discordgo.New("Bot " + *BotToken)
	s.AddHandler(func(s *discordgo.Session, r *discordgo.Ready) {
		fmt.Println("Bot is ready")
	})

	s.Identify.Intents = discordgo.MakeIntent(discordgo.IntentsAllWithoutPrivileged)

	err := s.Open()
	if err != nil {
		log.Fatalf("Cannot open the session: %v", err)
	}
	defer s.Close()

	createAmazingEvent(s)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	log.Println("Graceful shutdown")

}

// Create a new event on guild
func createAmazingEvent(s *discordgo.Session) {
	// Define the starting time (must be in future)
	startingTime := time.Now().Add(1 * time.Hour)
	// Define the ending time (must be after starting time)
	endingTime := startingTime.Add(30 * time.Minute)
	// Create the event
	scheduledEvent, err := s.GuildScheduledEventCreate(*GuildID, &discordgo.GuildScheduledEventParams{
		Name:               "Amazing Event",
		Description:        "This event will start in 1 hour and last 30 minutes",
		ScheduledStartTime: &startingTime,
		ScheduledEndTime:   &endingTime,
		EntityType:         discordgo.GuildScheduledEventEntityTypeExternal,
		EntityMetadata: &discordgo.GuildScheduledEventEntityMetadata{
			Location: "https://discord.com",
		},
		PrivacyLevel: discordgo.GuildScheduledEventPrivacyLevelGuildOnly,
	})
	if err != nil {
		log.Printf("Error creating scheduled event: %v", err)
		return
	}

	fmt.Println("Created scheduled event:", scheduledEvent.Image)
}
