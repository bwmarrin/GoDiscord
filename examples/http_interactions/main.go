package main

import (
	"encoding/hex"
	"flag"
	"log"
	"net/http"

	"github.com/bwmarrin/discordgo"
)

var (
	GuildID   = flag.String("guild", "", "Test guild ID. If not passed - bot registers commands globally")
	AppID     = flag.String("app", "", "Discord app ID")
	BotToken  = flag.String("token", "", "Bot access token")
	PublicKey = flag.String("publickey", "", "Public key for verifying requests")
)

func init() { flag.Parse() }

func main() {
	s, err := discordgo.New("Bot " + *BotToken)
	if err != nil {
		log.Fatalf("Invalid bot parameters: %v", err)
	}

	hexDecodedKey, err := hex.DecodeString(*PublicKey)
	if err != nil {
		log.Fatal("Invalid public key: ", err)
	}
	s.PublicKey = hexDecodedKey

	if _, err = s.ApplicationCommandBulkOverwrite(*AppID, *GuildID, []*discordgo.ApplicationCommand{
		{
			Name:        "ping",
			Description: "Ping command",
		},
	}); err != nil {
		log.Fatalf("Failed to register commands: %v", err)
	}

	s.AddHandler(handleInteraction)

	http.Handle("/", s)

	if err = http.ListenAndServe(":5678", nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func handleInteraction(s *discordgo.Session, e *discordgo.InteractionCreate) {
	if e.Type != discordgo.InteractionApplicationCommand {
		return
	}
	data := e.ApplicationCommandData()

	switch data.Name {
	case "ping":
		if err := e.Respond(&discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "pong",
			},
		}); err != nil {
			log.Print("Failed to respond to interaction: ", err)
		}
	}
}
