<img align="right" src="http://bwmarrin.github.io/discordgo/img/discordgo.png">

This example demonstrates how to utilize DiscordGo to manage threads
in a channel.

**Join [Discord Gophers](https://discord.gg/0f1SbxBZjYoCtNPP)
Discord chat channel for support.**

### Build

This assumes you already have a working Go environment setup and that
DiscordGo is correctly installed on your system.

From within the threads example folder, run the below command to compile the
example.

```sh
go build
```

### Usage

```
Usage of threads:
  -token string
    	Bot token
```

The below example shows how to start the bot from the threads example folder.

```sh
./threads -token YOUR_BOT_TOKEN
```