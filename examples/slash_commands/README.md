<img align="right" src="http://bwmarrin.github.io/discordgo/img/discordgo.png">

This example demonstrates how to utilize DiscordGo to create a Slash Command based bot,
which would be able to listen and respond to interactions. This example covers all aspects
of slash command interactions: options, choices, responses and followup messages.
Additionally, to avoid confusion, this example must be viewed more as a **step-by-step tutorial**,
than a demonstration bot.

**Join [Discord Gophers](https://discord.gg/0f1SbxBZjYoCtNPP)
Discord chat channel for support.**

### Build

This assumes you already have a working Go environment setup and that
DiscordGo is correctly installed on your system.

From within the slash_commands example folder, run the below command to compile the
example.

```sh
go build
```

### Usage

```
Usage of slash_commands:
  -guild string
    	Test guild ID. If not passed - bot registers commands globally
  -rmcmd
    	Whether to remove all commands after shutting down (default true)
  -token string
    	Bot access token
```

The below example shows how to start the bot from the slash_commands example folder.

```sh
./slash_commands -guild YOUR_TESTING_GUILD -token YOUR_BOT_TOKEN
```